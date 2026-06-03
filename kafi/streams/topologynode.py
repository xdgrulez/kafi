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
from pydbsp.operator import Differentiate, Input, Integrate, Lift1, Lift2, LiftStreamIntroduction
from pydbsp.relational_operators import (
    DeltaLiftedDeltaLiftedDistinct,
    DeltaLiftedDeltaLiftedJoin,
    LiftProject,
    LiftSelect,
)
from pydbsp.storage import DictStorage
from pydbsp.zset import ZSet, ZSetAddition

import uuid

import msgpack

#

default_pack_function = msgpack.packb
default_unpack_function = msgpack.unpackb

#

class TopologyNode:
    def __init__(self, name_str, daughter_tn_set, build_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        self._name_str = name_str
        self._id_str = str(uuid.uuid4())
        self._daughter_tn_set = daughter_tn_set
        self._build_function = build_function
        #
        self._pack_function = pack_function
        self._unpack_function = unpack_function
        #
        self._evaluator = None
        self._output_nodeId = None

    def build(self):
        evaluator = Evaluator(
            circuit=Circuit(),
            storage=DictStorage(),
            ctx=ComputeCtx(lattice=dbsp_time(2)),
            group=ZSetAddition())
        #
        self._foreach_bu(lambda tn: tn._build_function(evaluator))

    #

    def map(self, map_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        def _map_function(packed_value_any):
            value_any = self._unpack_function(packed_value_any)
            return self._pack_function(map_function(value_any))
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            input_nodeId = self._output_nodeId
            #
            liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, input_nodeId)
            liftProject_nodeId = LiftProject(f=_map_function).connect(evaluator.circuit, (liftStreamIntroduction_nodeId,))
            #
            tn._output_nodeId = liftProject_nodeId
        #
        tn = TopologyNode("map_op", {self}, _build_function, pack_function, unpack_function)
        #
        return tn

    def peek(self, peek_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        def _peek_function(packed_value_any):
            value_any = self._unpack_function(packed_value_any)
            #
            peek_function(value_any)
            #
            return packed_value_any
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            input_nodeId = self._output_nodeId
            #
            liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, input_nodeId)
            liftProject_nodeId = LiftProject(f=_peek_function).connect(evaluator.circuit, (liftStreamIntroduction_nodeId,))
            #
            tn._output_nodeId = liftProject_nodeId
        #
        tn = TopologyNode("peek_op", {self}, _build_function, pack_function, unpack_function)
        #
        return tn

    def flatmap(self, flatmap_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        def _flatmap_function(zSet):
            out_inner_dict = {}
            for packed_value_any, weight_int in zSet.inner.items():
                for value_any in flatmap_function(self._unpack_function(packed_value_any)):
                    packed_key_any = self._pack_function(value_any)
                    out_inner_dict[packed_key_any] = out_inner_dict.get(packed_key_any, 0) + weight_int
            return ZSet({packed_key_any: weight_int for packed_key_any, weight_int in out_inner_dict.items() if weight_int != 0})
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            input_nodeId = self._output_nodeId
            #
            integrate_nodeId = Integrate(group=g).connect(evaluator.circuit, (input_nodeId,))
            lift1_nodeId = Lift1(f=_flatmap_function).connect(evaluator.circuit, (integrate_nodeId,))
            differentiate_nodeId = Differentiate(group=g).connect(evaluator.circuit, (lift1_nodeId,))
            #
            tn._output_nodeId = differentiate_nodeId
        #
        tn = TopologyNode("flatmap_op", {self}, _build_function, pack_function, unpack_function)
        #
        return tn

    def filter(self, filter_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        def _filter_function(packed_value_any):
            value_any = self._unpack_function(packed_value_any)
            return filter_function(value_any)
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            input_nodeId = self._output_nodeId
            #
            liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, input_nodeId)
            liftSelect_nodeId = LiftSelect(pred=_filter_function).connect(evaluator.circuit, (liftStreamIntroduction_nodeId,))
            #
            tn._output_nodeId = liftSelect_nodeId
        #
        tn = TopologyNode("filter_op", {self}, _build_function, pack_function, unpack_function)
        #
        return tn

    def distinct(self, pack_function=default_pack_function, unpack_function=default_unpack_function):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            input_nodeId = self._output_nodeId
            #
            liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, input_nodeId)
            deltaLiftedDeltaLiftedDistinct_nodeId = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (liftStreamIntroduction_nodeId,))
            #
            tn._output_nodeId = deltaLiftedDeltaLiftedDistinct_nodeId
        #
        tn = TopologyNode("distinct_op", {self}, _build_function, pack_function, unpack_function)
        #
        return tn

    def union(self, other_tn, pack_function=default_pack_function, unpack_function=default_unpack_function):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = other_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            lift2_add_nodeId = Lift2(op=g.add).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId, r_liftStreamIntroduction_nodeId))
            deltaLiftedDeltaLiftedDistinct_nodeId = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (lift2_add_nodeId,))
            integrate_nodeId = Integrate(group=g).connect(evaluator.circuit, (deltaLiftedDeltaLiftedDistinct_nodeId,))
            #
            tn._output_nodeId = integrate_nodeId
        #
        tn = TopologyNode("union_op", {self, other_tn}, _build_function, pack_function, unpack_function)
        #
        return tn

    def intersect(self, other_tn, pack_function=default_pack_function, unpack_function=default_unpack_function):
        tn = self.join(other_tn, lambda l, r: l == r, lambda l, _: l, pack_function, unpack_function)
        tn._name = "intersect_op"
        #
        return tn

    def diff(self, other_tn, pack_function=default_pack_function, unpack_function=default_unpack_function):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = other_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, r_input_nodeId)
            r_lift1_neg_nodeId = Lift1(f=g.neg).connect(evaluator.circuit, (r_liftStreamIntroduction_nodeId,))
            lift2_add_nodeId = Lift2(op=g.add).connect(evaluator.circuit, (l_liftStreamIntroduction_nodeId, r_lift1_neg_nodeId))
            deltaLiftedDeltaLiftedDistinct_nodeId = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (lift2_add_nodeId,))
            #
            tn._output_nodeId = deltaLiftedDeltaLiftedDistinct_nodeId
        #
        tn = TopologyNode("diff_op", {self, other_tn}, _build_function, pack_function, unpack_function)
        #
        return tn

    def join(self, other_tn, predicate_function, projection_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        def _predicate_function(left_packed_value_any, right_packed_value_any):
            left_value_any = self._unpack_function(left_packed_value_any)
            right_value_any = self._unpack_function(right_packed_value_any)
            return predicate_function(left_value_any, right_value_any)
        #
        def _projection_function(left_packed_value_any, right_packed_value_any):
            left_value_any = self._unpack_function(left_packed_value_any)
            right_value_any = self._unpack_function(right_packed_value_any)
            return self._pack_function(projection_function(left_value_any, right_value_any))
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            l_input_nodeId = self._output_nodeId
            r_input_nodeId = other_tn._output_nodeId
            #
            l_liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, r_input_nodeId)
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
        tn = TopologyNode("join_op", {self, other_tn}, _build_function, pack_function, unpack_function)
        #
        return tn

    def join_equi(self, other_tn, left_select_function, right_select_function, projection_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        def _left_select_function(left_packed_value_any):
            left_value_any = self._unpack_function(left_packed_value_any)
            return self._pack_function(left_select_function(left_value_any))
        #
        def _right_select_function(right_packed_value_any):
            right_value_any = self._unpack_function(right_packed_value_any)
            return self._pack_function(right_select_function(right_value_any))
        #
        def _projection_function(_, left_packed_value_any, right_packed_value_any):
            left_value_any = self._unpack_function(left_packed_value_any)
            right_value_any = self._unpack_function(right_packed_value_any)
            return self._pack_function(projection_function(left_value_any, right_value_any))
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
            l_liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, l_input_nodeId)
            r_liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, r_input_nodeId)
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
        tn = TopologyNode("join_equi_op", {self, other_tn}, _build_function, pack_function, unpack_function)
        #
        return tn

    def group_by_agg(self, by_function, select_function, projection_function, agg_function, agg_initial_any, pydbsp_aggregate_function=None, pack_function=default_pack_function, unpack_function=default_unpack_function):
        def _by_function(packed_value_any):
            value_any = self._unpack_function(packed_value_any)
            return by_function(value_any)
        #
        def _select_function(packed_value_any):
            value_any = self._unpack_function(packed_value_any)
            return select_function(value_any)
        #
        def _projection_function(key_any_sum_any_tuple):
            key_any, sum_any = key_any_sum_any_tuple
            value_any = projection_function(key_any, sum_any)
            return self._pack_function(value_any)
        #
        def _default_pydbsp_aggregate_function(packed_value_any_weight_int_tuple_list):
            agg_any = agg_initial_any
            #
            for packed_value_any, weight_int in packed_value_any_weight_int_tuple_list:
                select_any = _select_function(packed_value_any)
                #
                agg_any = agg_function(agg_any, select_any, weight_int)
            #
            return agg_any
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
            liftStreamIntroduction_nodeId = self.liftStreamIntroduction(g, evaluator, input_nodeId)
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
        tn = TopologyNode("group_by_agg_op", {self}, _build_function, pack_function, unpack_function)
        #
        return tn

    #

    def group_by_sum(self, by_function, select_function, output_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        tn = self.group_by_agg(by_function, select_function, output_function, lambda x, y, z: x + y * z, 0, pack_function=pack_function, unpack_function=unpack_function)
        tn._name = "group_by_sum_op"
        #
        return tn

    def group_by_max(self, by_function, select_function, output_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        tn = self.group_by_agg(by_function, select_function, output_function, lambda x, y, _: max(x, y), None, pack_function=pack_function, unpack_function=unpack_function)
        tn._name = "group_by_max_op"
        #
        return tn

    def group_by_min(self, by_function, select_function, output_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        tn = self.group_by_agg(by_function, select_function, output_function, lambda x, y, _: min(x, y), None, pack_function=pack_function, unpack_function=unpack_function)
        tn._name = "group_by_min_op"
        #
        return tn

    def group_by_count(self, by_function, output_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        tn = self.group_by_sum(by_function, lambda _: 1, output_function, pack_function=pack_function, unpack_function=unpack_function)
        tn._name = "group_by_count_op"
        #
        return tn

    #

    def agg(self, select_function, output_function, agg_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        tn = self.group_by_agg(lambda _: 0, select_function, output_function, agg_function, pack_function=pack_function, unpack_function=unpack_function)
        tn._name = "agg_op"
        #
        return tn

    def sum(self, select_function, output_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        tn = self.group_by_sum(lambda _: 0, select_function, output_function, pack_function=pack_function, unpack_function=unpack_function)
        tn._name = "sum_op"
        #
        return tn

    def max(self, select_function, output_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        tn = self.group_by_max(lambda _: 0, select_function, output_function, pack_function=pack_function, unpack_function=unpack_function)
        tn._name = "max_op"
        #
        return tn

    def min(self, select_function, output_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        tn = self.group_by_min(lambda _: 0, select_function, output_function, pack_function=pack_function, unpack_function=unpack_function)
        tn._name = "min_op"
        #
        return tn

    def count(self, output_function, pack_function=default_pack_function, unpack_function=default_unpack_function):
        tn = self.group_by_count(lambda _: 0, output_function, pack_function=pack_function, unpack_function=unpack_function)
        tn._name = "count_op"
        #
        return tn

    #

    def latest(self, gc=True):
        gc_boolean = gc
        #
        zSet = self._evaluator.latest(self._output_nodeId)
        #
        if gc_boolean:
            self._evaluator.compact()
        #
        return zSet

    def push(self, source_str, value_any_list, weight_int=1):
        source_str_source_tn_dict = self.get_source_nodes()
        #
        for source_str_, source_tn in source_str_source_tn_dict.items():
            input_nodeId = source_tn._output_nodeId
            #
            if source_str_ == source_str:
                zSet = ZSet({self._pack_function(value_any): weight_int for value_any in value_any_list})
            else:
                zSet = ZSet({})
            #
            self._evaluator.push(input_nodeId, zSet)

    def step(self, gc=True, bag=False):
        bag_boolean = bag
        #
        zSet = self.latest(gc)
        #
        updated_value_any_list = []
        deleted_value_any_list = []
        for packed_value_any, weight_int in zSet.items():
            if weight_int > 0:
                if not bag_boolean:
                    weight_int = 1
                for _ in range(weight_int):
                    value_any = self._unpack_function(packed_value_any)
                    updated_value_any_list.append(value_any)
            elif weight_int < 0:
                if not bag_boolean:
                    weight_int = -1
                for _ in range(-weight_int):
                    value_any = self._unpack_function(packed_value_any)
                    deleted_value_any_list.append(value_any)
        #
        return updated_value_any_list, deleted_value_any_list

    #

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
        #
        def __filter_td(tn):
            if filter_function(tn):
                tn_set.add(tn)
            #
            for daughter_tn in tn._daughter_tn_set:
                __filter_td(daughter_tn)
        #
        __filter_td(self)
        #
        return tn_set

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
        tn_set = self._filter_td(lambda tn: len(tn._daughter_tn_set) == 0)
        #
        name_str_tn_dict = {tn._name_str: tn for tn in tn_set}
        #
        return name_str_tn_dict

    #

    def topology(self, include_ids=False):
        include_ids_bool = include_ids
        #
        daughters_int = len(self._daughter_tn_set)
        match daughters_int:
            case 0:
                if include_ids_bool:
                    return f"{self._name_str}_{self._id_str}"
                else:
                    return self._name_str
            case 1:
                if include_ids_bool:
                    return f"{self._name_str}_{self._id_str}({list(self._daughter_tn_set)[0].topology(include_ids_bool)})"
                else:
                    return f"{self._name_str}({list(self._daughter_tn_set)[0].topology(include_ids_bool)})"
            case 2:
                if include_ids_bool:
                    return  f"{self._name_str}_{self._id_str}({list(self._daughter_tn_set)[0].topology(include_ids_bool)}, {list(self._daughter_tn_set)[1].topology(include_ids_bool)})"
                else:
                    return  f"{self._name_str}({list(self._daughter_tn_set)[0].topology(include_ids_bool)}, {list(self._daughter_tn_set)[1].topology(include_ids_bool)})"

    def mermaid(self, include_ids=False):
        include_ids_bool = include_ids
        #
        mermaid_edge_str_set = set()
        #
        def collect_edges(tn):
            for daughter_tn in tn._daughter_tn_set:
                if include_ids_bool:
                    mermaid_edge_str = f"{tn._id_str}[{tn._name_str}_{tn._id_str}] --> {daughter_tn._id_str}[{daughter_tn._name_str}_{tn._id_str}]\n"
                else:
                    mermaid_edge_str = f"{tn._id_str}[{tn._name_str}] --> {daughter_tn._id_str}[{daughter_tn._name_str}]\n"
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

    @staticmethod
    def source(source_str, pack_function=default_pack_function, unpack_function=default_unpack_function):
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            input = Input(frontier=Antichain(dbsp_time(1))).connect(evaluator.circuit, ())
            #
            tn._output_nodeId = input
        #
        tn = TopologyNode(source_str, [], _build_function, pack_function, unpack_function)
        #
        return tn

    @staticmethod
    def liftStreamIntroduction(g, evaluator, i_in):
        return i_in if evaluator.frontiers()[i_in].lattice.nestedness == 2 else LiftStreamIntroduction(group=g).connect(evaluator.circuit, (i_in,))

#

source = TopologyNode.source
