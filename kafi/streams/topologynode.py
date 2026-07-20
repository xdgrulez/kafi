from pydbsp.progress import Feedback as ProgressFeedback
from types import SimpleNamespace


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

default_pack_fun = msgpack.packb
default_unpack_fun = msgpack.unpackb

#

class TopologyNode:
    def __init__(self, name_str, daughter_tn_set, build_fun, **kwargs):
        self._name_str = name_str
        self._id_str = str(uuid.uuid4())
        self._daughter_tn_set = daughter_tn_set
        self._build_fun = build_fun
        #
        self._evaluator = None
        self._output_nodeId = None
        #
        self._pack_fun = kwargs["pack_fun"] if "pack_fun" in kwargs else default_pack_fun
        self._unpack_fun = kwargs["unpack_fun"] if "unpack_fun" in kwargs else default_unpack_fun
        #
        self._to_zSet_fun = kwargs["to_zSet_fun"] if "to_zSet_fun" in kwargs else self.from_records
        self._from_zSet_fun = kwargs["from_zSet_fun"] if "from_zSet_fun" in kwargs else self.to_records
        #
        self._source_str = None
        self._sink_str = None
        self._sink_str_list = None
        #
        self._reset_fun = None

    ###
    # DBSP base operators
    ###

    def _integrate(self, **kwargs):
        def _build_fun(evaluator):
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
        tn = current_class("_integrate_op", {self}, _build_fun, **kwargs)
        #
        return tn

    def _differentiate(self, **kwargs):
        def _build_fun(evaluator):
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
        tn = current_class("_differentiate_op", {self}, _build_fun, **kwargs)
        #
        return tn

    def _delay(self, **kwargs):
        def _build_fun(evaluator):
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
        tn = current_class("_delay_op", {self}, _build_fun, **kwargs)
        #
        return tn

    ###
    # Relational operators
    ###

    # Map

    def _map(self, _map_fun, **kwargs):
        def __map_fun(zSet):
            out_inner_dict = {}
            for packed_r, w in zSet.inner.items():
                out_r, out_w = _map_fun(tn._unpack_fun(packed_r), w)
                #
                if out_w != 0:
                    out_packed_r = tn._pack_fun(out_r)
                    out_inner_dict[out_packed_r] = out_w
            #
            return ZSet(out_inner_dict)
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            input_nodeId = self._output_nodeId
            #
            lift1_nodeId = Lift1(f=__map_fun).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = lift1_nodeId

        #
        current_class = type(self)
        tn = current_class("_map_op", {self}, _build_fun, **kwargs)
        #
        return tn

    def map(self, map_fun, **kwargs):
        def _map_fun(r, w):
            out_r = map_fun(r)
            #
            return out_r, w
        #
        tn = self._map(_map_fun, **kwargs)
        tn._name_str = "map_op"
        #
        return tn

    def peek(self, description_str=None, peek_fun=None, **kwargs):
        def map_fun(r):
            peek_fun(r)
            #
            return r
        #
        if peek_fun is None:
            peek_fun = lambda x: print(x) if description_str is None else print(f"{description_str}: {x}")
        #
        tn = self.map(map_fun, **kwargs)
        tn._name_str = "peek_op"
        #
        return tn

    def _peek(self, description_str=None, _peek_fun=None, **kwargs):
        def _map_fun(r, w):
            _peek_fun(r, w)
            #
            return r, w
        #
        if _peek_fun is None:
            _peek_fun = lambda x, y: print((x, y)) if description_str is None else print(f"{description_str}: {(x, y)}")
        #
        tn = self._map(_map_fun, **kwargs)
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
        def _map_fun(r, w):
            return r, -w
        #
        tn = self._map(_map_fun, **kwargs)
        tn._name_str = "_neg_op"
        #
        return tn

    # Flatmap

    def _flatmap(self, _flatmap_fun, **kwargs):
        def __flatmap_fun(zSet):
            out_inner_dict = {}
            for packed_r, w in zSet.inner.items():
                for out_r, out_w in _flatmap_fun(tn._unpack_fun(packed_r), w):
                    out_packed_key_any = tn._pack_fun(out_r)
                    out_inner_dict[out_packed_key_any] = out_inner_dict.get(out_packed_key_any, 0) + out_w
            return ZSet({out_packed_key_any: out_w for out_packed_key_any, out_w in out_inner_dict.items() if out_w != 0})
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            input_nodeId = self._output_nodeId
            #
            lift1_nodeId = Lift1(f=__flatmap_fun).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = lift1_nodeId
        #
        current_class = type(self)
        tn = current_class("_flatmap_op", {self}, _build_fun, **kwargs)
        #
        return tn

    def flatmap(self, flatmap_fun, **kwargs):
        def _flatmap_fun(r, w):
            out_r_list = flatmap_fun(r)
            #
            return [(out_r, w) for out_r in out_r_list]
        #
        tn = self._flatmap(_flatmap_fun, **kwargs)
        tn._name_str = "flatmap_op"
        #
        return tn

    # Filter

    def _filter(self, _filter_fun, **kwargs):
        def __filter_fun(zSet):
            out_inner_dict = {}
            for packed_r, w in zSet.inner.items():
                if _filter_fun(tn._unpack_fun(packed_r), w):
                    out_inner_dict[packed_r] = w
            #
            return ZSet(out_inner_dict)
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            input_nodeId = self._output_nodeId
            #
            lift1_nodeId = Lift1(f=__filter_fun).connect(evaluator.circuit, (input_nodeId,))
            #
            tn._output_nodeId = lift1_nodeId
        #
        current_class = type(self)
        tn = current_class("_filter_op", {self}, _build_fun, **kwargs)
        #
        return tn

    def filter(self, filter_fun, **kwargs):
        def _filter_fun(r, _):
            return filter_fun(r)
        #
        tn = self._filter(_filter_fun, **kwargs)
        tn._name_str = "filter_op"
        #
        return tn

    # Distinct

    def distinct(self, **kwargs):
        def _build_fun(evaluator):
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
        tn = current_class("distinct_op", {self}, _build_fun, **kwargs)
        #
        return tn

    # Union

    def union(self, other_tn, **kwargs):
        def _build_fun(evaluator):
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
        tn = current_class("union_op", {self, other_tn}, _build_fun, **kwargs)
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
        def _build_fun(evaluator):
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
        tn = current_class("diff_op", {self, other_tn}, _build_fun, **kwargs)
        #
        return tn
    
    # Join

    def join(self, other_tn, predicate_fun, projection_fun, **kwargs):
        def _predicate_fun(left_packed_r, right_packed_r):
            left_r = tn._unpack_fun(left_packed_r)
            right_r = tn._unpack_fun(right_packed_r)
            return predicate_fun(left_r, right_r)
        #
        def _projection_fun(left_packed_r, right_packed_r):
            left_r = tn._unpack_fun(left_packed_r)
            right_r = tn._unpack_fun(right_packed_r)
            return tn._pack_fun(projection_fun(left_r, right_r))
        #
        def _build_fun(evaluator):
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
                pred=_predicate_fun,
                proj=_projection_fun,
                group_a=g,
                group_b=g,
                out_group=g,
            ).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId, r_liftStreamIntroduction_nodeId))
            #
            tn._output_nodeId = deltaLiftedDeltaLiftedJoin_nodeId
        #
        current_class = type(self)
        tn = current_class("join_op", {self, other_tn}, _build_fun, **kwargs)
        #
        return tn

    def join_equi(self, other_tn, left_select_fun, right_select_fun, projection_fun, **kwargs):
        def _left_select_fun(left_packed_r):
            left_r = tn._unpack_fun(left_packed_r)
            return tn._pack_fun(left_select_fun(left_r))
        #
        def _right_select_fun(right_packed_r):
            right_r = tn._unpack_fun(right_packed_r)
            return tn._pack_fun(right_select_fun(right_r))
        #
        def _projection_fun(_, left_packed_r, right_packed_r):
            left_r = tn._unpack_fun(left_packed_r)
            right_r = tn._unpack_fun(right_packed_r)
            return tn._pack_fun(projection_fun(left_r, right_r))
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            l_g_idx = IndexedZSetAddition(g, _left_select_fun)
            r_g_idx = IndexedZSetAddition(g, _right_select_fun)
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = other_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            l_liftIndex_nodeId = LiftIndex(indexer=_left_select_fun).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId,))
            r_liftIndex_nodeId = LiftIndex(indexer=_right_select_fun).connect(evaluator.circuit, (r_liftStreamIntroduction_nodeId,))
            indexedDeltaLiftedDeltaLiftedJoin_nodeId = IndexedDeltaLiftedDeltaLiftedJoin(
                proj=_projection_fun,
                group_a=l_g_idx,
                group_b=r_g_idx,
                out_group=g,
            ).connect(evaluator.circuit, (l_liftIndex_nodeId, r_liftIndex_nodeId))
            #
            tn._output_nodeId = indexedDeltaLiftedDeltaLiftedJoin_nodeId
        #
        current_class = type(self)
        tn = current_class("join_equi_op", {self, other_tn}, _build_fun, **kwargs)
        #
        return tn
    
    # Group By + Aggregation

    def group_by_agg(self, by_fun, select_fun, agg_fun, agg_initial, projection_fun, **kwargs):
        def _by_fun(packed_r):
            r = tn._unpack_fun(packed_r)
            return tn._pack_fun(by_fun(r))
        #
        def _select_fun(packed_r):
            r = tn._unpack_fun(packed_r)
            return tn._pack_fun(select_fun(r))
        #
        def _projection_fun(packed_key_any_packed_sum_any_tuple):
            packed_key_any, packed_sum_any = packed_key_any_packed_sum_any_tuple
            r = projection_fun(tn._unpack_fun(packed_key_any), tn._unpack_fun(packed_sum_any))
            return tn._pack_fun(r)
        #
        def _agg_fun(packed_r_w_tuple_list):
            packed_agg_any = _agg_initial
            #
            for packed_r, _ in packed_r_w_tuple_list:
                packed_select_any = _select_fun(packed_r)
                #
                agg_any = tn._unpack_fun(packed_agg_any)
                select_any = tn._unpack_fun(packed_select_any)
                #
                packed_agg_any = tn._pack_fun(agg_fun(agg_any, select_any))
            #
            return packed_agg_any
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            g_idx = IndexedZSetAddition[str, str](g, _by_fun)
            #
            input_nodeId = self._output_nodeId
            #
            liftStreamIntroduction_nodeId = tn.liftStreamIntroduction(g, evaluator, input_nodeId)
            liftLiftIndex_nodeId = LiftLiftIndex(indexer=_by_fun).connect(evaluator.circuit, (liftStreamIntroduction_nodeId,))
            deltaLiftedDeltaLiftedGroupBy_nodeId = DeltaLiftedDeltaLiftedGroupBy(
                aggregate=_agg_fun,
                group=g_idx,
                out_group=g,
            ).connect(evaluator.circuit, (liftLiftIndex_nodeId,))
            liftProject_nodeId = LiftProject(f=_projection_fun).connect(evaluator.circuit, (deltaLiftedDeltaLiftedGroupBy_nodeId,))
            integrate_nodeId = Integrate(group=g).connect(evaluator.circuit, (liftProject_nodeId,))
            differentiate_nodeId = Differentiate(group=g).connect(evaluator.circuit, (integrate_nodeId,))
            #
            tn._output_nodeId = differentiate_nodeId
        #
        current_class = type(self)
        tn = current_class("group_by_agg_op", {self}, _build_fun, **kwargs)
        #
        _agg_initial = tn._pack_fun(agg_initial)
        #
        return tn

    def group_by_sum(self, by_fun, select_fun, projection_fun, sum_initial_any=0, **kwargs):
        tn = self.group_by_agg(by_fun, select_fun, lambda agg, x: agg + x, sum_initial_any, projection_fun, **kwargs)
        tn._name_str = "group_by_sum_op"
        #
        return tn

    def group_by_max(self, by_fun, select_fun, projection_fun, max_initial_any=0, **kwargs):
        tn = self.group_by_agg(by_fun, select_fun, lambda agg, x: max(agg, x), max_initial_any, projection_fun, **kwargs)
        tn._name_str = "group_by_max_op"
        #
        return tn

    def group_by_min(self, by_fun, select_fun, projection_fun, min_initial_any=0, **kwargs):
        tn = self.group_by_agg(by_fun, select_fun, lambda agg, x: min(agg, x), min_initial_any, projection_fun, **kwargs)
        tn._name_str = "group_by_min_op"
        #
        return tn

    def group_by_count(self, by_fun, projection_fun, **kwargs):
        tn = self.group_by_sum(by_fun, lambda _: 1, projection_fun, **kwargs)
        tn._name_str = "group_by_count_op"
        #
        return tn

    # Aggregation

    def agg(self, select_fun, agg_fun, agg_initial, projection_fun, **kwargs):
        tn = self.group_by_agg(lambda _: 0, select_fun, agg_fun, agg_initial, lambda _, x: projection_fun(x), **kwargs)
        tn._name_str = "agg_op"
        #
        return tn

    def sum(self, select_fun, projection_fun=lambda x: x, sum_initial_any=0, **kwargs):
        tn = self.agg(select_fun, lambda agg, x: agg + x, sum_initial_any, projection_fun, **kwargs)
        tn._name_str = "sum_op"
        #
        return tn

    def max(self, select_fun, projection_fun=lambda x: x, max_initial_any=0, **kwargs):
        tn = self.agg(select_fun, lambda agg, x: max(agg, x), max_initial_any, projection_fun, **kwargs)
        tn._name_str = "max_op"
        #
        return tn

    def min(self, select_fun, projection_fun=lambda x: x, min_initial_any=0, **kwargs):
        tn = self.agg(select_fun, lambda agg, x: min(agg, x), min_initial_any, projection_fun, **kwargs)
        tn._name_str = "min_op"
        #
        return tn

    def count(self, projection_fun=lambda x: x, **kwargs):
        tn = self.sum(lambda _: 1, projection_fun, **kwargs)
        tn._name_str = "count_op"
        #
        return tn

    ###
    # Merge
    ###

    def merge(self, other_tn, **kwargs):
        def _build_fun(evaluator):
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
        tn = current_class("merge_op", {self, other_tn}, _build_fun, **kwargs)
        #
        return tn

    ###
    # Time Windows - Expiry
    ###

    def expire(self, time_fun, expiry_fun, projection_fun=lambda x: x[0], **kwargs):
        input_plus_expiry_tn = (
            self
            .map(lambda x: (x, expiry_fun(time_fun(x))), **kwargs)
        )
        #
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #   
            NEG_INF = float("-inf")
            g = ZSetAddition()
            input_nodeId = input_plus_expiry_tn._output_nodeId
            #
            def _ts_fun(packed_r):
                r, _ = tn._unpack_fun(packed_r)
                return time_fun(r)
            #
            def _expiry_fun(packed_r):
                _, expiry_int = tn._unpack_fun(packed_r)
                return expiry_int
            #
            def _compute(t, reads, ctx):
                read_input, read_self = reads
                #                
                chain = ctx.lattice.factors[0]
                predecessor = chain.predecessor(t[0])
                #
                if predecessor is None:
                    prev_zSet, prev_max_ts_int = g.identity(), NEG_INF
                else:
                    pred_t = (predecessor,) + t[1:]
                    prev_zSet, prev_max_ts_int, _ = read_self(pred_t)
                #
                input_zSet = read_input(t)
                #
                input_max_ts_int = max(
                    (_ts_fun(k) for k, w in input_zSet.items() if w > 0),
                    default=None,
                )
                max_ts_int = prev_max_ts_int if input_max_ts_int is None else max(prev_max_ts_int, input_max_ts_int)
                #
                merged_zSet = g.add(prev_zSet, input_zSet)
                #                
                new_state_dict = {}
                expired_dict = {}
                #
                for k, w in merged_zSet.items():
                    if w == 0:
                        continue
                    if _expiry_fun(k) > max_ts_int:
                        new_state_dict[k] = w
                    else:
                        expired_dict[k] = -w
                #                
                new_state_zSet = ZSet(new_state_dict)
                expired_zSet = ZSet(expired_dict)
                #
                delta_zSet = g.add(input_zSet, expired_zSet)
                #
                return new_state_zSet, max_ts_int, delta_zSet
            #
            next_nodeId = evaluator.circuit.next_id()
            expire_nodeId = evaluator.circuit.add(
                ProgressFeedback(input=input_nodeId, self_id=next_nodeId, axis=0),
                SimpleNamespace(compute=_compute),
            )
            #
            project_nodeId = Lift1(f=lambda tup: tup[2]).connect(
                evaluator.circuit, (expire_nodeId,))
            #            
            tn._output_nodeId = project_nodeId
        #
        current_class = type(self)
        tn = current_class("expire_op", {input_plus_expiry_tn}, _build_fun, **kwargs)
        #
        return tn.map(projection_fun, **kwargs)

    ###
    # Time Windows - Trigger
    ###

    def trigger(self, time_tn, time_fun, trigger_fun=lambda r_end_ts_int_tuple, latest_ts_int: latest_ts_int >= r_end_ts_int_tuple[1], projection_fun=lambda r_end_ts_int_tuple: r_end_ts_int_tuple[0], positive_only=True, **kwargs):
        positive_only_boolean = positive_only
        #
        trigger_tn = (
            self
            .join(time_tn.max(time_fun),
                  lambda r_end_ts_int_tuple, latest_ts_int: trigger_fun(r_end_ts_int_tuple, latest_ts_int),
                  lambda r_end_ts_int_tuple, _: projection_fun(r_end_ts_int_tuple),
                  **kwargs)
        )
        trigger_tn = trigger_tn._filter(lambda _, w: w > 0, **kwargs) if positive_only_boolean else trigger_tn
        #
        return trigger_tn

    ###
    # Time Windows - Assign end of window(s) for a timestamp
    ###

    @staticmethod
    def _assign_tumbling(size_int):
        def _assign_fun(ts_int):
            return [(ts_int // size_int) * size_int + size_int]
        #
        return _assign_fun

    @staticmethod
    def _assign_hopping(size_int, hop_int):
        def _assign_fun(ts_int):
            first_end_ts_int = (ts_int // hop_int) * hop_int + hop_int
            #
            return [first_end_ts_int + i * hop_int
                    for i in range(size_int // hop_int) if first_end_ts_int + i * hop_int >= size_int]
        #
        return _assign_fun

    @staticmethod
    def _assign_cumulative(size_int, advance_int):
        def _assign_fun(ts_int):
            cumulative_start_int = (ts_int // size_int) * size_int
            first_step_end_ts_int = ((ts_int // advance_int) * advance_int) + advance_int
            cumulative_end_ts_int = cumulative_start_int + size_int
            #
            return [step_end_ts_int
                    for step_end_ts_int in range(first_step_end_ts_int, cumulative_end_ts_int + advance_int, advance_int)
                    if step_end_ts_int <= cumulative_end_ts_int]
        #
        return _assign_fun

    @staticmethod
    def _assign_sliding(size_int):
        def _assign_fun(ts_int):
            return [ts_int + size_int]
        #
        return _assign_fun

    @staticmethod
    def _assign_session(max_session_int):
        def _assign_fun(ts_int):
            return [(ts_int // max_session_int) * max_session_int + max_session_int]
        #
        return _assign_fun

    ###
    # Time Windows - Group By + Agg
    ###

    def _group_by_agg_aligned(self, assign_fun, time_fun, by_fun, agg_fun, agg_initial, projection_fun, **kwargs):
        _projection_fun = lambda by_r_end_ts_int_tuple, agg_r: (projection_fun(by_r_end_ts_int_tuple[0], agg_r), by_r_end_ts_int_tuple[1])
        #
        tn = (
            self
            .flatmap(lambda r: [(r, end_ts_int) for end_ts_int in assign_fun(time_fun(r))], **kwargs)
            .group_by_agg(
                lambda r_end_ts_int_tuple: (by_fun(r_end_ts_int_tuple[0]), r_end_ts_int_tuple[1]),
                lambda r_end_ts_int_tuple: r_end_ts_int_tuple[0],
                agg_fun,
                agg_initial,
                _projection_fun,
                **kwargs)
        )
        #
        return tn
    
    #

    def _group_by_agg_sliding(self, assign_fun, time_fun, by_fun, agg_fun, agg_initial, projection_fun, **kwargs):
        tn = (
            self
            .map(lambda r: (r, assign_fun(time_fun(r))[0]))
            .group_by_agg(lambda r_end_ts_int_tuple: by_fun(r_end_ts_int_tuple[0]),
                          lambda r_end_ts_int_tuple: r_end_ts_int_tuple,
                          lambda agg_r_end_ts_int_tuple, r_end_ts_int_tuple: 
                          (agg_fun(agg_r_end_ts_int_tuple[0], r_end_ts_int_tuple[0]), r_end_ts_int_tuple[1]),
                          (agg_initial, 0),
                          lambda by, agg_r_end_ts_int_tuple: 
                          (projection_fun(by, agg_r_end_ts_int_tuple[0]), agg_r_end_ts_int_tuple[1]),
                          **kwargs)
        )
        #
        return tn

    #

    def _group_by_agg_session(self, gap_int, time_fun, by_fun, agg_fun, agg_initial, projection_fun, **kwargs):
        def insert_session(r, session_dict_list):
            ts_int = time_fun(r)
            #
            left_session_dict = next((session_dict 
                                      for session_dict in session_dict_list 
                                      if session_dict["start"] - gap_int <= ts_int <= session_dict["last_ts"] + gap_int), None)
            #
            if left_session_dict:
                left_session_dict["records"].append(r)
                left_session_dict["start"] = min(left_session_dict["start"], ts_int)
                left_session_dict["last_ts"] = max(left_session_dict["last_ts"], ts_int)
                left_session_dict["agg"] = agg_fun(left_session_dict["agg"], r)
                #
                right_session_dict = next((session_dict 
                                           for session_dict in session_dict_list 
                                           if session_dict != left_session_dict 
                                           and session_dict["start"] - gap_int <= left_session_dict["last_ts"] + gap_int 
                                           and left_session_dict["start"] - gap_int <= session_dict["last_ts"]), None)
                if right_session_dict:
                    left_session_dict["records"].extend(right_session_dict["records"])
                    left_session_dict["start"] = min(left_session_dict["start"], right_session_dict["start"])
                    left_session_dict["last_ts"] = max(left_session_dict["last_ts"], right_session_dict["last_ts"])
                    #
                    left_session_dict["records"].sort(key=time_fun)
                    #                    
                    agg_any = agg_initial.copy()
                    for r in left_session_dict["records"]:
                        agg_any = agg_fun(agg_any, r)
                    left_session_dict["agg"] = agg_any
                    #
                    session_dict_list.remove(right_session_dict)
            else:
                session_dict_list.append({
                    "start": ts_int,
                    "last_ts": ts_int,
                    "records": [r],
                    "agg": agg_fun(agg_initial.copy(), r)
                })
            #
            session_dict_list.sort(key=lambda session_dict: session_dict["start"])
            #
            return session_dict_list
        #
        def _flatmap_fun(by_any_agg_any_session_end_ts_int_tuple_list):
            return [(projection_fun(by_any_agg_any_session_end_ts_int_tuple_list[0], agg_any_session_end_ts_int_tuple[0]), agg_any_session_end_ts_int_tuple[1])
                    for agg_any_session_end_ts_int_tuple in by_any_agg_any_session_end_ts_int_tuple_list[1]]
        #
        tn = (
            self
            .group_by_agg(
                by_fun,
                lambda r: r,
                lambda agg, r:
                {"sessions": (session_dict_list := insert_session(r, agg.get("sessions", []))),
                 "output": [(session_dict["agg"], session_dict["last_ts"] + gap_int) for session_dict in session_dict_list]},
                {"sessions": [], "output": []},
                lambda by, agg_r: (by, agg_r["output"]),
                **kwargs
            )
            .flatmap(_flatmap_fun, **kwargs)
        )
        return tn

    ###
    # Time Windows - Expiry
    ###

    def _expire_window(self, time_fun, assign_fun, allowed_lateness_int=0, **kwargs):
        return self.expire(time_fun,
                              lambda r: max(assign_fun(r)) + allowed_lateness_int,
                              **kwargs)
    
    #

    def expire_tumbling(self, time_fun, size_int, allowed_lateness_int=0, **kwargs):
        return self._expire_window(time_fun,
                                     TopologyNode._assign_tumbling(size_int),
                                     allowed_lateness_int,
                                     **kwargs)

    def expire_hopping(self, time_fun, size_int, hop_int, allowed_lateness_int=0, **kwargs):
        return self._expire_window(time_fun,
                                     TopologyNode._assign_hopping(size_int, hop_int),
                                     allowed_lateness_int,
                                     **kwargs)

    def expire_cumulative(self, time_fun, size_int, advance_int, allowed_lateness_int=0, **kwargs):
        return self._expire_window(time_fun,
                                     TopologyNode._assign_cumulative(size_int, advance_int),
                                     allowed_lateness_int,
                                     **kwargs)
    
    def expire_sliding(self, time_fun, size_int, **kwargs):
        return self._expire_window(time_fun,
                                     TopologyNode._assign_sliding(size_int),
                                     **kwargs)

    def expire_session(self, time_fun, max_session_int, allowed_lateness_int=0, **kwargs):
        return self._expire_window(time_fun,
                                     TopologyNode._assign_session(max_session_int),
                                     allowed_lateness_int,
                                     **kwargs)

    ###
    # Time Windows - {Group By, Trigger}
    ###

    def _window_aligned(self, assign_fun, time_fun, by_fun, agg_fun, agg_initial, projection_fun, trigger_projection_fun=lambda r_end_ts_int_tuple: r_end_ts_int_tuple[0], trigger_fun=lambda r_end_ts_int_tuple, latest_ts_int: latest_ts_int >= r_end_ts_int_tuple[1], trigger_positive_only=True, **kwargs):
        group_by_agg_tn = (
            self
            ._group_by_agg_aligned(assign_fun,
                                   time_fun,
                                   by_fun,
                                   agg_fun,
                                   agg_initial,
                                   projection_fun,
                                   **kwargs)
        )
        #
        trigger_tn = group_by_agg_tn.trigger(self,
                                             time_fun,
                                             trigger_fun,
                                             trigger_projection_fun,
                                             trigger_positive_only,
                                             **kwargs)
        #
        return trigger_tn

    #

    def window_tumbling(self, size_int, time_fun, by_fun, agg_fun, agg_initial, projection_fun, trigger_projection_fun=lambda r_end_ts_int_tuple: r_end_ts_int_tuple[0], trigger_fun=lambda r_end_ts_int_tuple, latest_ts_int: latest_ts_int >= r_end_ts_int_tuple[1], trigger_positive_only=True, **kwargs):
        return self._window_aligned(TopologyNode._assign_tumbling(size_int),
                                    time_fun,
                                    by_fun,
                                    agg_fun,
                                    agg_initial,
                                    projection_fun,
                                    trigger_projection_fun,
                                    trigger_fun,
                                    trigger_positive_only,
                                    **kwargs)

    def window_hopping(self, size_int, hop_int, time_fun, by_fun, agg_fun, agg_initial, projection_fun, trigger_projection_fun=lambda r_end_ts_int_tuple: r_end_ts_int_tuple[0], trigger_fun=lambda r_end_ts_int_tuple, latest_ts_int: latest_ts_int >= r_end_ts_int_tuple[1], trigger_positive_only=True, **kwargs):
        return self._window_aligned(TopologyNode._assign_hopping(size_int, hop_int),
                                    time_fun,
                                    by_fun,
                                    agg_fun,
                                    agg_initial,
                                    projection_fun,
                                    trigger_projection_fun,
                                    trigger_fun,
                                    trigger_positive_only,
                                    **kwargs)

    def window_cumulative(self, size_int, advance_int, time_fun, by_fun, agg_fun, agg_initial, projection_fun, trigger_projection_fun=lambda r_end_ts_int_tuple: r_end_ts_int_tuple[0], trigger_fun=lambda r_end_ts_int_tuple, latest_ts_int: latest_ts_int >= r_end_ts_int_tuple[1], trigger_positive_only=True, **kwargs):
        return self._window_aligned(TopologyNode._assign_cumulative(size_int, advance_int),
                                    time_fun,
                                    by_fun,
                                    agg_fun,
                                    agg_initial,
                                    projection_fun,
                                    trigger_projection_fun,
                                    trigger_fun,
                                    trigger_positive_only,
                                    **kwargs)

    #

    def window_sliding(self, size_int, time_fun, by_fun, agg_fun, agg_initial, projection_fun, trigger_projection_fun=lambda r_end_ts_int_tuple: r_end_ts_int_tuple[0], trigger_positive_only=True, **kwargs):
        trigger_positive_only_boolean = trigger_positive_only
        #
        group_by_agg_tn = self._group_by_agg_sliding(TopologyNode._assign_sliding(size_int),
                                                     time_fun,
                                                     by_fun,
                                                     agg_fun,
                                                     agg_initial,
                                                     projection_fun,
                                                     **kwargs)
        trigger_tn = group_by_agg_tn.map(trigger_projection_fun)
        #
        trigger_tn = trigger_tn._filter(lambda _, w: w > 0) if trigger_positive_only_boolean else trigger_tn
        #
        return trigger_tn
    
    #

    def window_session(self, gap_int, time_fun, by_fun, agg_fun, agg_initial, projection_fun, trigger_projection_fun=lambda r_end_ts_int_tuple: r_end_ts_int_tuple[0], trigger_fun=lambda r_end_ts_int_tuple, latest_ts_int: latest_ts_int >= r_end_ts_int_tuple[1], trigger_positive_only=True, **kwargs):
        group_by_agg_tn = (
            self
            ._group_by_agg_session(gap_int,
                                   time_fun,
                                   by_fun, 
                                   agg_fun,
                                   agg_initial,
                                   projection_fun,
                                   **kwargs)
        )
        #
        trigger_tn = group_by_agg_tn.trigger(self,
                                             time_fun,
                                             trigger_fun,
                                             trigger_projection_fun,
                                             trigger_positive_only,
                                             **kwargs)
        #
        return trigger_tn

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
        def _build_fun(evaluator):
            tn._evaluator = evaluator
            #
            input = Input(frontier=Antichain(dbsp_time(1))).connect(evaluator.circuit, ())
            #
            tn._output_nodeId = input
        #
        tn = TopologyNode(f"source_{source_str}", {}, _build_fun, **kwargs)
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
        def get_map_fun(sink_str):
            return lambda x: (sink_str, x)
        #
        for sink_str, sink_root_tn in tail_sink_str_sink_tn_tuple_list:
            root_tn = root_tn.merge(sink_root_tn.map(get_map_fun(sink_str)))
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
        def _reset_fun():
            evaluator = root_tn._get_evaluator()
            #
            root_tn._foreach_bu(lambda tn: tn._build_fun(evaluator))
        #
        _reset_fun()
        #
        root_tn._reset_fun = _reset_fun
        #
        return root_tn

    def reset(self):
        if self._reset_fun is None:
            raise Exception("Not built yet.")
        #
        self._reset_fun()

    # Input

    def push(self, source_str_input_any_list_dict_or_source_str, input_any_list=None):
        if input_any_list is None:
            source_str_input_any_list_dict = source_str_input_any_list_dict_or_source_str
        else:
            source_str_input_any_list_dict = {source_str_input_any_list_dict_or_source_str: input_any_list}
        #
        source_str_source_tn_dict = self.get_source_nodes()
        #
        for source_str, source_tn in source_str_source_tn_dict.items():
            input_any_list = source_str_input_any_list_dict.get(source_str, [])
            #
            input_nodeId = source_tn._output_nodeId
            #
            zSet = source_tn._to_zSet_fun(input_any_list, self._pack_fun)
            #
            self._evaluator.push(input_nodeId, zSet)

    @staticmethod
    def _from_records(r_w_tuple_list, pack_fun):
        zSet = ZSet({pack_fun(r): w for r, w in r_w_tuple_list})
        #
        return zSet

    @staticmethod
    def from_records(r_list, pack_fun):
        zSet = ZSet({pack_fun(r): 1 for r in r_list})
        #
        return zSet

    @staticmethod
    def from_debezium(message_dict_list, pack_fun):
        inner_dict = {}
        for message_dict in message_dict_list:
            if message_dict["value"]["op"] in ["c", "u"]:
                message_dict1 = copy.deepcopy(message_dict)
                message_dict1["value"] = message_dict["value"]["after"]
                inner_dict[pack_fun(message_dict1)] = 1
            elif message_dict["value"]["op"] == "d":
                message_dict1 = copy.deepcopy(message_dict)
                message_dict1["value"] = message_dict["value"]["before"]
                inner_dict[pack_fun(message_dict1)] = -1
        #
        return ZSet(inner_dict)
    
    def to_zSet(self, to_zSet_fun):
        self._to_zSet_fun = to_zSet_fun
    
    # Output

    def latest(self, gc=True):
        gc_boolean = gc
        #
        zSet = self._evaluator.latest(self._output_nodeId)
        #
        if gc_boolean:
            self._evaluator.compact()
        #
        unpacked_zSet = [(self._unpack_fun(packed_r), w) for packed_r, w in zSet.items()]
        #
        if self._sink_str_list is None:
            output_any = self._from_zSet_fun(unpacked_zSet)
        else:
            sink_str_unpacked_r_w_tuple_list_dict = defaultdict(list)
            for (sink_str, unpacked_r), w in unpacked_zSet:
                sink_str_unpacked_r_w_tuple_list_dict[sink_str].append((unpacked_r, w))
            #
            output_any = {sink_str: self._from_zSet_fun(unpacked_r_w_tuple_list) for sink_str, unpacked_r_w_tuple_list in sink_str_unpacked_r_w_tuple_list_dict.items()} 
        #
        return output_any
     
    def from_zSet(self, from_zSet_fun):
        self._from_zSet_fun = from_zSet_fun
            
    @staticmethod
    def _to_records(unpacked_r_w_tuple_list):
        return unpacked_r_w_tuple_list

    @staticmethod
    def to_records(unpacked_r_w_tuple_list):
        r_list = []
        for unpacked_r, w in unpacked_r_w_tuple_list:
            if w > 0:
                for _ in range(w):
                    r_list.append(unpacked_r)
        #
        return r_list

    @staticmethod
    def to_debezium(unpacked_r_w_tuple_list):
        message_dict_list = []
        for message_dict, w in unpacked_r_w_tuple_list:
            if w > 0:
                for _ in range(w):
                    message_dict1 = copy.deepcopy(message_dict)
                    message_dict1["value"]["before"] = None
                    message_dict1["value"]["after"] = message_dict["value"]
                    message_dict1["value"]["op"] = "c"
                    message_dict_list.append(message_dict1)
            elif w < 0:
                for _ in range(-w):
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

    def _foreach_bu(self, foreach_fun):
        visited_tn_set = set()
        #
        def __foreach_bu(tn):
            if tn not in visited_tn_set:
                visited_tn_set.add(tn)
                #
                for daughter_tn in tn._daughter_tn_set:
                    __foreach_bu(daughter_tn)
                #
                foreach_fun(tn)
        #
        __foreach_bu(self)

    def _filter_td(self, filter_fun):
        tn_set = set()
        visited_tn_set = set()
        #
        def __filter_td(tn):
            if tn in visited_tn_set:
                return
            visited_tn_set.add(tn)
            #
            if filter_fun(tn):
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

    def get_source_nodes(self):
        tn_set = self._filter_td(lambda tn: tn._source_str is not None)
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
