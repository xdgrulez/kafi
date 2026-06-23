from pydbsp.circuit import Circuit
from pydbsp.compute import ComputeCtx
from pydbsp.core import Antichain, dbsp_time
from pydbsp.evaluate import Evaluator
from pydbsp.indexed_relational_operators import (
    DeltaLiftedDeltaLiftedGroupBy,
    IndexedDeltaLiftedDeltaLiftedJoin,
    LiftIndex,
    LiftLiftIndex,
)
from pydbsp.indexed_zset import IndexedZSetAddition
from pydbsp.operator import Delay, Differentiate, Input, Integrate, Lift1, Lift2, LiftStreamIntroduction
from pydbsp.relational_operators import (
    DeltaLiftedDeltaLiftedDistinct,
    DeltaLiftedDeltaLiftedJoin,
    LiftProject
)
from pydbsp.storage import DictStorage
from pydbsp.zset import ZSet, ZSetAddition

import copy, uuid
from collections import defaultdict

import msgpack

#

default_pack_function = msgpack.packb
default_unpack_function = msgpack.unpackb

#

class TopologyNode:
    def __init__(self, name_str, daughter_tn_set, build_function, **kwargs):
        self._name_str = name_str
        self._id_str = str(uuid.uuid4())
        self._daughter_tn_set = daughter_tn_set
        self._build_function = build_function
        #
        self._evaluator = None
        self._output_nodeId = None
        #
        self._pack_function = kwargs["pack_function"] if "pack_function" in kwargs else default_pack_function
        self._unpack_function = kwargs["unpack_function"] if "unpack_function" in kwargs else default_unpack_function
        #
        self._to_zSet_function = kwargs["to_zSet_function"] if "to_zSet_function" in kwargs else self.from_records
        self._from_zSet_function = kwargs["from_zSet_function"] if "from_zSet_function" in kwargs else self.to_records
        #
        self._expired_tn = None
        #
        self._source_str = None
        self._sink_str = None
        self._sink_str_list = None

    ###
    # DBSP base operators
    ###

    def _integrate(self, **kwargs):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            input_nodeId = self._output_nodeId
            #
            integrate_nodeId = Integrate(group=g).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = integrate_nodeId
        #
        current_class = type(self)
        tn = current_class("_integrate_op", {self}, _build_function, **kwargs)
        #
        return tn

    def _differentiate(self, **kwargs):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            input_nodeId = self._output_nodeId
            #
            differentiate_nodeId = Differentiate(group=g).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = differentiate_nodeId
        #
        current_class = type(self)
        tn = current_class("_differentiate_op", {self}, _build_function, **kwargs)
        #
        return tn

    def _delay(self, **kwargs):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            input_nodeId = self._output_nodeId
            #
            integrate_nodeId = Delay(group=g).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = integrate_nodeId
        #
        current_class = type(self)
        tn = current_class("_delay_op", {self}, _build_function, **kwargs)
        #
        return tn

    ###
    # Relational operators
    ###

    # Map

    def _map(self, _map_function, **kwargs):
        def __map_function(zSet):
            out_inner_dict = {}
            for packed_record_any, weight_int in zSet.inner.items():
                out_record_any, out_weight_int = _map_function(tn._unpack_function(packed_record_any), weight_int)
                #
                if out_weight_int != 0:
                    out_packed_record_any = tn._pack_function(out_record_any)
                    out_inner_dict[out_packed_record_any] = out_weight_int
            #
            return ZSet(out_inner_dict)
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            input_nodeId = self._output_nodeId
            #
            lift1_nodeId = Lift1(f=__map_function).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = lift1_nodeId

        #
        current_class = type(self)
        tn = current_class("_map_op", {self}, _build_function, **kwargs)
        #
        return tn

    def map(self, map_function, **kwargs):
        def _map_function(record_any, weight_int):
            out_record_any = map_function(record_any)
            #
            return out_record_any, weight_int
        #
        tn = self._map(_map_function, **kwargs)
        tn._name_str = "map_op"
        #
        return tn

    def peek(self, description_str=None, peek_function=None, **kwargs):
        def map_function(record_any):
            peek_function(record_any)
            #
            return record_any
        #
        if peek_function is None:
            peek_function = lambda x: print(x) if description_str is None else print(f"{description_str}: {x}")
        #
        tn = self.map(map_function, **kwargs)
        tn._name_str = "peek_op"
        #
        return tn

    def _peek(self, description_str=None, _peek_function=None, **kwargs):
        def _map_function(record_any, weight_int):
            _peek_function(record_any, weight_int)
            #
            return record_any, weight_int
        #
        if _peek_function is None:
            _peek_function = lambda x, y: print((x, y)) if description_str is None else print(f"{description_str}: {(x, y)}")
        #
        tn = self._map(_map_function, **kwargs)
        tn._name_str = "_peek_op"
        #
        return tn

    def from_value(self, **kwargs):
        tn = self.map(lambda x: x["value"], **kwargs)
        tn._name_str = "from_value_op"
        #
        return tn

    def to_value(self, **kwargs):
        tn = self.map(lambda x: {"value": x}, **kwargs)
        tn._name_str = "to_value_op"
        #
        return tn

    def _neg(self, **kwargs):
        def _map_function(record_any, weight_int):
            return record_any, -weight_int
        #
        tn = self._map(_map_function, **kwargs)
        tn._name_str = "_neg_op"
        #
        return tn

    # Flatmap

    def _flatmap(self, _flatmap_function, **kwargs):
        def __flatmap_function(zSet):
            out_inner_dict = {}
            for packed_record_any, weight_int in zSet.inner.items():
                for out_record_any, out_weight_int in _flatmap_function(tn._unpack_function(packed_record_any), weight_int):
                    out_packed_key_any = tn._pack_function(out_record_any)
                    out_inner_dict[out_packed_key_any] = out_inner_dict.get(out_packed_key_any, 0) + out_weight_int
            return ZSet({out_packed_key_any: out_weight_int for out_packed_key_any, out_weight_int in out_inner_dict.items() if out_weight_int != 0})
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            input_nodeId = self._output_nodeId
            #
            lift1_nodeId = Lift1(f=__flatmap_function).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = lift1_nodeId
        #
        current_class = type(self)
        tn = current_class("_flatmap_op", {self}, _build_function, **kwargs)
        #
        return tn

    def flatmap(self, flatmap_function, **kwargs):
        def _flatmap_function(record_any, weight_int):
            out_record_any_list = flatmap_function(record_any)
            #
            return [(out_record_any, weight_int) for out_record_any in out_record_any_list]
        #
        tn = self._flatmap(_flatmap_function, **kwargs)
        tn._name_str = "flatmap_op"
        #
        return tn

    # Filter

    def _filter(self, _filter_function, **kwargs):
        def __filter_function(zSet):
            out_inner_dict = {}
            for packed_record_any, weight_int in zSet.inner.items():
                if _filter_function(tn._unpack_function(packed_record_any), weight_int):
                    out_inner_dict[packed_record_any] = weight_int
            #
            return ZSet(out_inner_dict)
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            input_nodeId = self._output_nodeId
            #
            lift1_nodeId = Lift1(f=__filter_function).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = lift1_nodeId
        #
        current_class = type(self)
        tn = current_class("_filter_op", {self}, _build_function, **kwargs)
        #
        return tn

    def filter(self, filter_function, **kwargs):
        def _filter_function(record_any, weight_int):
            return filter_function(record_any)
        #
        tn = self._filter(_filter_function, **kwargs)
        tn._name_str = "filter_op"
        #
        return tn

    # Distinct

    def distinct(self, **kwargs):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            input_nodeId = self._output_nodeId
            #
            liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, input_nodeId)
            deltaLiftedDeltaLiftedDistinct_nodeId = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (liftStreamIntroduction_nodeId,))
            #
            tn._output_nodeId = deltaLiftedDeltaLiftedDistinct_nodeId
        #
        current_class = type(self)
        tn = current_class("distinct_op", {self}, _build_function, **kwargs)
        #
        return tn

    # Union

    def union(self, other_tn, **kwargs):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = other_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            lift2_add_nodeId = Lift2(op=g.add).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId, r_liftStreamIntroduction_nodeId))
            deltaLiftedDeltaLiftedDistinct_nodeId = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (lift2_add_nodeId,))
            integrate_nodeId = Integrate(group=g).connect(evaluator.circuit, (deltaLiftedDeltaLiftedDistinct_nodeId,))
            #
            tn._output_nodeId = integrate_nodeId
        #
        current_class = type(self)
        tn = current_class("union_op", {self, other_tn}, _build_function, **kwargs)
        #
        return tn

    # Intersect

    def intersect(self, other_tn, **kwargs):
        tn = self.join(other_tn, lambda l, r: l == r, lambda l, _: l, **kwargs)
        tn._name_str = "intersect_op"
        #
        return tn

    # Minus

    def minus(self, other_tn, **kwargs):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = other_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            r_lift1_neg_nodeId = Lift1(f=g.neg).connect(evaluator.circuit, (r_liftStreamIntroduction_nodeId,))
            lift2_add_nodeId = Lift2(op=g.add).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId, r_lift1_neg_nodeId))
            deltaLiftedDeltaLiftedDistinct_nodeId = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (lift2_add_nodeId,))
            #
            tn._output_nodeId = deltaLiftedDeltaLiftedDistinct_nodeId
        #
        current_class = type(self)
        tn = current_class("diff_op", {self, other_tn}, _build_function, **kwargs)
        #
        return tn
    
    # Join

    def join(self, other_tn, predicate_function, projection_function, **kwargs):
        def _predicate_function(left_packed_record_any, right_packed_record_any):
            left_record_any = tn._unpack_function(left_packed_record_any)
            right_record_any = tn._unpack_function(right_packed_record_any)
            return predicate_function(left_record_any, right_record_any)
        #
        def _projection_function(left_packed_record_any, right_packed_record_any):
            left_record_any = tn._unpack_function(left_packed_record_any)
            right_record_any = tn._unpack_function(right_packed_record_any)
            return tn._pack_function(projection_function(left_record_any, right_record_any))
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = other_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            deltaLiftedDeltaLiftedJoin_nodeId = DeltaLiftedDeltaLiftedJoin(
                pred=_predicate_function,
                proj=_projection_function,
                group_a=g,
                group_b=g,
                out_group=g,
            ).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId, r_liftStreamIntroduction_nodeId))
            #
            tn._output_nodeId = deltaLiftedDeltaLiftedJoin_nodeId
        #
        current_class = type(self)
        tn = current_class("join_op", {self, other_tn}, _build_function, **kwargs)
        #
        return tn

    def join_equi(self, other_tn, left_select_function, right_select_function, projection_function, **kwargs):
        def _left_select_function(left_packed_record_any):
            left_record_any = tn._unpack_function(left_packed_record_any)
            return tn._pack_function(left_select_function(left_record_any))
        #
        def _right_select_function(right_packed_record_any):
            right_record_any = tn._unpack_function(right_packed_record_any)
            return tn._pack_function(right_select_function(right_record_any))
        #
        def _projection_function(_, left_packed_record_any, right_packed_record_any):
            left_record_any = tn._unpack_function(left_packed_record_any)
            right_record_any = tn._unpack_function(right_packed_record_any)
            return tn._pack_function(projection_function(left_record_any, right_record_any))
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            l_g_idx = IndexedZSetAddition(g, _left_select_function)
            r_g_idx = IndexedZSetAddition(g, _right_select_function)
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = other_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            l_liftIndex_nodeId = LiftIndex(indexer=_left_select_function).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId,))
            r_liftIndex_nodeId = LiftIndex(indexer=_right_select_function).connect(evaluator.circuit, (r_liftStreamIntroduction_nodeId,))
            indexedDeltaLiftedDeltaLiftedJoin_nodeId = IndexedDeltaLiftedDeltaLiftedJoin(
                proj=_projection_function,
                group_a=l_g_idx,
                group_b=r_g_idx,
                out_group=g,
            ).connect(evaluator.circuit, (l_liftIndex_nodeId, r_liftIndex_nodeId))
            #
            tn._output_nodeId = indexedDeltaLiftedDeltaLiftedJoin_nodeId
        #
        current_class = type(self)
        tn = current_class("join_equi_op", {self, other_tn}, _build_function, **kwargs)
        #
        return tn
    
    # Group By + Aggregation

    def group_by_agg(self, by_function, select_function, projection_function, agg_function, agg_initial_any, pydbsp_aggregate_function=None, **kwargs):
        def _by_function(packed_record_any):
            record_any = tn._unpack_function(packed_record_any)
            return tn._pack_function(by_function(record_any))
        #
        def _select_function(packed_record_any):
            record_any = tn._unpack_function(packed_record_any)
            return tn._pack_function(select_function(record_any))
        #
        def _projection_function(packed_key_any_packed_sum_any_tuple):
            packed_key_any, packed_sum_any = packed_key_any_packed_sum_any_tuple
            record_any = projection_function(tn._unpack_function(packed_key_any), tn._unpack_function(packed_sum_any))
            return tn._pack_function(record_any)
        #
        def _agg_function(packed_agg_any, packed_select_any, weight_int):
            agg_any = tn._unpack_function(packed_agg_any)
            select_any = tn._unpack_function(packed_select_any)
            return tn._pack_function(agg_function(agg_any, select_any, weight_int))
        #
        def _default_pydbsp_aggregate_function(packed_record_any_weight_int_tuple_list):
            packed_agg_any = _agg_initial_any
            #
            for packed_record_any, weight_int in packed_record_any_weight_int_tuple_list:
                packed_select_any = _select_function(packed_record_any)
                #
                packed_agg_any = _agg_function(packed_agg_any, packed_select_any, weight_int)
            #
            return packed_agg_any
        #
        _pydbsp_aggregate_function = _default_pydbsp_aggregate_function if pydbsp_aggregate_function is None else pydbsp_aggregate_function
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            g_idx = IndexedZSetAddition[str, str](g, _by_function)
            #
            input_nodeId = self._output_nodeId
            #
            liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, input_nodeId)
            liftLiftIndex_nodeId = LiftLiftIndex(indexer=_by_function).connect(evaluator.circuit, (liftStreamIntroduction_nodeId,))
            deltaLiftedDeltaLiftedGroupBy_nodeId = DeltaLiftedDeltaLiftedGroupBy(
                aggregate=_pydbsp_aggregate_function,
                group=g_idx,
                out_group=g,
            ).connect(evaluator.circuit, (liftLiftIndex_nodeId,))
            liftProject_nodeId = LiftProject(f=_projection_function).connect(evaluator.circuit, (deltaLiftedDeltaLiftedGroupBy_nodeId,))
            integrate_nodeId = Integrate(group=g).connect(evaluator.circuit, (liftProject_nodeId,))
            differentiate_nodeId = Differentiate(group=g).connect(evaluator.circuit, (integrate_nodeId,))
            #
            tn._output_nodeId = differentiate_nodeId
        #
        current_class = type(self)
        tn = current_class("group_by_agg_op", {self}, _build_function, **kwargs)
        #
        _agg_initial_any = tn._pack_function(agg_initial_any)
        #
        return tn

    def group_by_sum(self, by_function, select_function, projection_function, sum_initial_any=0, **kwargs):
        tn = self.group_by_agg(by_function, select_function, projection_function, lambda x, y, z: x + y * z, sum_initial_any, **kwargs)
        tn._name_str = "group_by_sum_op"
        #
        return tn

    def group_by_max(self, by_function, select_function, projection_function, max_initial_any=0, **kwargs):
        tn = self.group_by_agg(by_function, select_function, projection_function, lambda x, y, _: max(x, y), max_initial_any, **kwargs)
        tn._name_str = "group_by_max_op"
        #
        return tn

    def group_by_min(self, by_function, select_function, projection_function, min_initial_any=0, **kwargs):
        tn = self.group_by_agg(by_function, select_function, projection_function, lambda x, y, _: min(x, y), min_initial_any, **kwargs)
        tn._name_str = "group_by_min_op"
        #
        return tn

    def group_by_count(self, by_function, projection_function, **kwargs):
        tn = self.group_by_sum(by_function, lambda _: 1, projection_function, **kwargs)
        tn._name_str = "group_by_count_op"
        #
        return tn

    # Aggregation

    def agg(self, select_function, projection_function, agg_function, agg_initial_any, **kwargs):
        tn = self.group_by_agg(lambda _: 0, select_function, lambda _, y: projection_function(y), agg_function, agg_initial_any, **kwargs)
        tn._name_str = "agg_op"
        #
        return tn

    def sum(self, select_function, projection_function, sum_initial_any=0, **kwargs):
        tn = self.agg(select_function, projection_function, lambda x, y, z: x + y * z, sum_initial_any, **kwargs)
        tn._name_str = "sum_op"
        #
        return tn

    def max(self, select_function, projection_function, max_initial_any=0, **kwargs):
        tn = self.agg(select_function, projection_function, lambda x, y, _: max(x, y), max_initial_any, **kwargs)
        tn._name_str = "max_op"
        #
        return tn

    def min(self, select_function, projection_function, min_initial_any=0, **kwargs):
        tn = self.agg(select_function, projection_function, lambda x, y, _: min(x, y), min_initial_any, **kwargs)
        tn._name_str = "min_op"
        #
        return tn

    def count(self, projection_function, **kwargs):
        tn = self.sum(lambda _: 1, projection_function, **kwargs)
        tn._name_str = "count_op"
        #
        return tn

    ###
    # Merge
    ###

    def merge(self, other_tn, **kwargs):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = other_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            lift2_add_nodeId = Lift2(op=g.add).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId, r_liftStreamIntroduction_nodeId))
            #
            tn._output_nodeId = lift2_add_nodeId
        #
        current_class = type(self)
        tn = current_class("merge_op", {self, other_tn}, _build_function, **kwargs)
        #
        return tn

    ###
    # Expiration
    ###

    def expire(self, get_time_function, get_expiry_function, out_function=lambda x: x[0], late_arrival=True, **kwargs):
        late_arrival_boolean = late_arrival
        #
        expire_source_str = f"expire_{uuid.uuid4()}"
        expire_source_tn = TopologyNode.source(expire_source_str, **kwargs)
        #
        input_with_expiry_tn = (
            self
            .map(lambda x: (x, get_expiry_function(x)))
        )
        #
        merged_input_with_expiry_tn = (
            input_with_expiry_tn
            .merge(expire_source_tn)
        )
        #
        input_now_tn = (
            merged_input_with_expiry_tn
            .map(lambda x: get_time_function(x[0]))
            .max(lambda x: x,
                 lambda x: x)
        )
        #
        _expired_tn = (
            merged_input_with_expiry_tn
            .join(input_now_tn,
                lambda l, r: r > l[1],
                lambda l, _: l)
            ._filter(lambda _, w: w > 0)
            ._neg()
        )
        if late_arrival_boolean:
            expired_tn = _expired_tn._delay()
        else:
            expire_tn = _expired_tn
        #
        expire_tn = (
            expired_tn
            .merge(input_with_expiry_tn)
            .map(out_function)
        )
        #
        expire_source_tn.to_zSet(TopologyNode._from_records)
        expired_tn.from_zSet(TopologyNode._to_records)
        expire_source_tn._expired_tn = expired_tn
        #
        return expire_tn

    ###
    # Operator utils
    ###

    @staticmethod
    def liftStreamIntroduction(g, evaluator, i_in):
        return i_in if evaluator.frontiers()[i_in].lattice.nestedness == 2 else LiftStreamIntroduction(group=g).connect(evaluator.circuit, (i_in,))

    ###
    # Sources and sinks
    ###

    @staticmethod
    def source(source_str, **kwargs):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            input = Input(frontier=Antichain(dbsp_time(1))).connect(evaluator.circuit, ())
            #
            tn._output_nodeId = input
        #
        tn = TopologyNode(f"source_{source_str}", {}, _build_function, **kwargs)
        tn._source_str = source_str
        #
        return tn

    def sink(self, sink_str):
        self._sink_str = sink_str
        #
        return self

    #

    @staticmethod
    def _build_root_tn(*sink_tn_tuple):
        sink_str_sink_tn_tuple_list = [(sink_tn._sink_str, sink_tn) for sink_tn in sink_tn_tuple if sink_tn._sink_str is not None]
        if sink_str_sink_tn_tuple_list == []:
            if len(sink_tn_tuple) == 1:
                return sink_tn_tuple[0]
            else:
                raise Exception("Cannot build multiple non-sink nodes.")
        #
        head_sink_str_sink_tn_tuple, *tail_sink_str_sink_tn_tuple_list = sink_str_sink_tn_tuple_list
        #
        head_sink_str, head_sink_tn = head_sink_str_sink_tn_tuple
        root_tn = head_sink_tn.map(lambda x: (head_sink_str, x))
        root_tn._name_str = f"sink_{head_sink_str}"
        #
        # We need this little factory to avoid unwanted variable shadowing for sink_str in the loop below.
        def get_map_function(sink_str):
            return lambda x: (sink_str, x)
        #
        for sink_str, sink_root_tn in tail_sink_str_sink_tn_tuple_list:
            root_tn = root_tn.merge(sink_root_tn.map(get_map_function(sink_str)))
            root_tn._name_str = f"sink_{sink_str}"
        #
        sink_str_list = [sink_str for sink_str, _ in sink_str_sink_tn_tuple_list]
        root_tn._sink_str_list = sink_str_list
        #
        return root_tn


    def _get_evaluator(self):
        evaluator = Evaluator(
            circuit=Circuit(),
            storage=DictStorage(),
            ctx=ComputeCtx(lattice=dbsp_time(2)),
            group=ZSetAddition())
        #
        return evaluator

    @staticmethod
    def build(*sink_tn_tuple):
        if sink_tn_tuple is None:
            raise Exception("At least one sink node required.")
        #
        root_tn = TopologyNode._build_root_tn(*sink_tn_tuple)
        #
        evaluator = root_tn._get_evaluator()
        #
        root_tn._foreach_bu(lambda tn: tn._build_function(evaluator))
        #
        return root_tn

    # Input

    def push(self, source_str_input_any_list_dict_or_source_str, input_any_list=None):
        if input_any_list is None:
            source_str_input_any_list_dict = source_str_input_any_list_dict_or_source_str
        else:
            source_str_input_any_list_dict = {source_str_input_any_list_dict_or_source_str: input_any_list}
        #
        source_str_source_tn_dict = self.get_source_nodes(True)
        #
        for source_str, source_tn in source_str_source_tn_dict.items():
            if source_tn._expired_tn is not None:
                input_any_list = source_tn._expired_tn.latest()
            else:
                input_any_list = source_str_input_any_list_dict.get(source_str, [])
            #
            input_nodeId = source_tn._output_nodeId
            #
            zSet = source_tn._to_zSet_function(input_any_list, self._pack_function)
            #
            self._evaluator.push(input_nodeId, zSet)

    @staticmethod
    def _from_records(record_any_weight_int_tuple_list, pack_function):
        zSet = ZSet({pack_function(record_any): weight_int for record_any, weight_int in record_any_weight_int_tuple_list})
        #
        return zSet

    @staticmethod
    def from_records(record_any_list, pack_function):
        zSet = ZSet({pack_function(record_any): 1 for record_any in record_any_list})
        #
        return zSet

    @staticmethod
    def from_debezium(message_dict_list, pack_function):
        inner_dict = {}
        for message_dict in message_dict_list:
            if message_dict["value"]["op"] in ["c", "u"]:
                message_dict1 = copy.deepcopy(message_dict)
                message_dict1["value"] = message_dict["value"]["after"]
                inner_dict[pack_function(message_dict1)] = 1
            elif message_dict["value"]["op"] == "d":
                message_dict1 = copy.deepcopy(message_dict)
                message_dict1["value"] = message_dict["value"]["before"]
                inner_dict[pack_function(message_dict1)] = -1
        #
        return ZSet(inner_dict)
    
    def to_zSet(self, to_zSet_function):
        self._to_zSet_function = to_zSet_function
    
    # Output

    def latest(self, gc=True):
        gc_boolean = gc
        #
        zSet = self._evaluator.latest(self._output_nodeId)
        #
        if gc_boolean:
            self._evaluator.compact()
        #
        unpacked_zSet = [(self._unpack_function(packed_record_any), weight_int) for packed_record_any, weight_int in zSet.items()]
        #
        if self._sink_str_list is None:
            output_any = self._from_zSet_function(unpacked_zSet)
        else:
            sink_str_unpacked_zSet_dict = defaultdict(list)
            for (sink_str, unpacked_record_any), weight_int in unpacked_zSet:
                sink_str_unpacked_zSet_dict[sink_str].append((unpacked_record_any, weight_int))
            #
            output_any = {sink_str: self._from_zSet_function(unpacked_zSet) for sink_str, unpacked_zSet in sink_str_unpacked_zSet_dict.items()} 
        #
        return output_any
     
    def from_zSet(self, from_zSet_function):
        self._from_zSet_function = from_zSet_function
            
    @staticmethod
    def _to_records(unpacked_zSet):
        return unpacked_zSet

    @staticmethod
    def to_records(unpacked_zSet):
        record_any_list = []
        for unpacked_record_any, weight_int in unpacked_zSet:
            if weight_int > 0:
                for _ in range(weight_int):
                    record_any_list.append(unpacked_record_any)
        #
        return record_any_list

    @staticmethod
    def to_debezium(unpacked_zSet):
        message_dict_list = []
        for message_dict, weight_int in unpacked_zSet:
            if weight_int > 0:
                for _ in range(weight_int):
                    message_dict1 = copy.deepcopy(message_dict)
                    message_dict1["value"]["before"] = None
                    message_dict1["value"]["after"] = message_dict["value"]
                    message_dict1["value"]["op"] = "c"
                    message_dict_list.append(message_dict1)
            elif weight_int < 0:
                for _ in range(-weight_int):
                    message_dict1 = copy.deepcopy(message_dict)
                    message_dict1["value"]["before"] = message_dict["value"]
                    message_dict1["value"]["after"] = None
                    message_dict1["value"]["op"] = "d"
                    message_dict_list.append(message_dict1)
        #
        return message_dict_list

    ###
    # Helpers
    ###

    def _foreach_bu(self, foreach_function):
        visited_tn_set = set()
        #
        def __foreach_bu(tn):
            if tn not in visited_tn_set:
                visited_tn_set.add(tn)
                #
                for daughter_tn in tn._daughter_tn_set:
                    __foreach_bu(daughter_tn)
                #
                foreach_function(tn)
        #
        __foreach_bu(self)

    def _filter_td(self, filter_function):
        tn_set = set()
        visited_tn_set = set()
        #
        def __filter_td(tn):
            if tn in visited_tn_set:
                return
            visited_tn_set.add(tn)
            #
            if filter_function(tn):
                tn_set.add(tn)
            #
            for daughter_tn in tn._daughter_tn_set:
                __filter_td(daughter_tn)
        #
        __filter_td(self)
        #
        return tn_set

    #

    def get_id(self):
        return self._id_str
    
    def get_name(self):
        return self._name_str

    def get_daughters(self):
        return self._daughter_tn_set

    #

    def get_node_by_id(self, id_str):
        tn_set = self._filter_td(lambda tn: tn._id_str == id_str)
        #
        if len(tn_set) == 0:
            return None
        else:
            return list(tn_set)[0]
    
    def get_node_by_name(self, name_str):
        tn_set = self._filter_td(lambda tn: tn._name_str == name_str)
        #
        if len(tn_set) == 0:
            return None
        else:
            return list(tn_set)[0]

    def get_source_nodes(self, include_expire_boolean=False):
        tn_set = self._filter_td(lambda tn:
                                 tn._source_str is not None and
                                 (True if include_expire_boolean else tn._expired_tn is None))
        #
        name_str_tn_dict = {tn._source_str: tn for tn in tn_set}
        #
        return name_str_tn_dict

    def get_sink_nodes(self):
        tn_set = self._filter_td(lambda tn: tn._sink_str is not None)
        #
        name_str_tn_dict = {tn._sink_str: tn for tn in tn_set}
        #
        return name_str_tn_dict


    #

    def topology(self, include_ids=False, visited_tn_set=None):
        if visited_tn_set is None:
            visited_tn_set = set()
        #
        if self in visited_tn_set:
            if include_ids:
                return f"REF:{self._name_str}_{self._id_str}"
            else:
                return f"REF:{self._name_str}"
        #       
        visited_tn_set.add(self)
        #        
        include_ids_bool = include_ids
        daughters_int = len(self._daughter_tn_set)
        #
        daughters_list = list(self._daughter_tn_set)
        #
        match daughters_int:
            case 0:
                if include_ids_bool:
                    return f"{self._name_str}_{self._id_str}"
                else:
                    return self._name_str
            case 1:
                daughter_str = daughters_list[0].topology(include_ids_bool, visited_tn_set)
                if include_ids_bool:
                    return f"{self._name_str}_{self._id_str}({daughter_str})"
                else:
                    return f"{self._name_str}({daughter_str})"
            case 2:
                d1_str = daughters_list[0].topology(include_ids_bool, visited_tn_set)
                d2_str = daughters_list[1].topology(include_ids_bool, visited_tn_set)
                if include_ids_bool:
                    return f"{self._name_str}_{self._id_str}({d1_str}, {d2_str})"
                else:
                    return f"{self._name_str}({d1_str}, {d2_str})"

    def mermaid(self, include_ids=False):
        include_ids_bool = include_ids
        mermaid_edge_str_set = set()
        visited_tn_set = set()
        #
        def collect_edges(tn):
            if tn in visited_tn_set:
                return
            visited_tn_set.add(tn)
            #
            for daughter_tn in tn._daughter_tn_set:
                if include_ids_bool:
                    mermaid_edge_str = f"{daughter_tn._id_str}[{daughter_tn._name_str}_{tn._id_str}] --> {tn._id_str}[{tn._name_str}_{tn._id_str}]\n"
                else:
                    mermaid_edge_str = f"{daughter_tn._id_str}[{daughter_tn._name_str}] --> {tn._id_str}[{tn._name_str}]\n"
                #
                mermaid_edge_str_set.add(mermaid_edge_str)
                #
                collect_edges(daughter_tn)
        #
        collect_edges(self)
        #
        mermaid_top_str = "```mermaid\ngraph TD\n"
        mermaid_edges_str = "".join(mermaid_edge_str_set)
        mermaid_bottom_str = "```"
        #
        return mermaid_top_str + mermaid_edges_str + mermaid_bottom_str
