from pydbsp.circuit import Circuit
from pydbsp.compute import ComputeCtx
from pydbsp.core import Antichain, dbsp_time
from pydbsp.evaluate import Evaluator
from pydbsp.operator import Input, Lift1, LiftStreamIntroduction
from pydbsp.relational_operators import (
    DeltaLiftedDeltaLiftedDistinct,
    DeltaLiftedDeltaLiftedJoin,
    LiftProject,
    LiftSelect
)
from pydbsp.operator import Differentiate, Integrate
from pydbsp.storage import DictStorage
from pydbsp.zset import ZSet, ZSetAddition
from pydbsp.indexed_relational_operators import (
    IndexedDeltaLiftedDeltaLiftedJoin,
    LiftIndex,
)
from pydbsp.indexed_zset import IndexedZSetAddition

import datetime
import json
# import ujson as json
import uuid

#

class TopologyNode:
    def __init__(self, name_str, daughter_tn_set, build_function, serialize_function=json.dumps, deserialize_function=json.loads):
        self._name_str = name_str
        self._id_str = str(uuid.uuid4())
        self._daughter_tn_set = daughter_tn_set
        self._build_function = build_function
        #
        self._serialize_function = serialize_function
        self._deserialize_function = deserialize_function
        #
        self._evaluator = None
        self._output = None

    def build(self):
        evaluator = Evaluator(
            circuit=Circuit(),
            storage=DictStorage(),
            ctx=ComputeCtx(lattice=dbsp_time(2)),
            group=ZSetAddition())
        #
        self._foreach_bu(lambda tn: tn._build_function(evaluator))

    #

    def map(self, map_function):
        def _map_function(serialized_value_any):
            value_any = self._deserialize_function(serialized_value_any)
            return self._serialize_function(map_function(value_any))
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            i_in = self._output
            #
            i_2d = liftStreamIntroduction(g, evaluator, i_in)
            proj = LiftProject(f=_map_function).connect(evaluator.circuit, (i_2d,))
            selection = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (proj,))
            #
            tn._output = selection
        #
        tn = TopologyNode("map_op", {self}, _build_function)
        #
        return tn

    def flatmap(self, flatmap_function):
        def _expand(zSet):
            out = {}
            for value_json_str, weight in zSet.inner.items():
                value_dict = json.loads(value_json_str)
                for output_dict in flatmap_function(value_dict):
                    key = json.dumps(output_dict)
                    out[key] = out.get(key, 0) + weight
            return ZSet({k: v for k, v in out.items() if v != 0})
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            i_in = self._output
            #
            cum = Integrate(group=g).connect(evaluator.circuit, (i_in,))
            expanded = Lift1(f=_expand).connect(evaluator.circuit, (cum,))
            diffs = Differentiate(group=g).connect(evaluator.circuit, (expanded,))
            #
            tn._output = diffs
        #
        tn = TopologyNode("flatmap_op", {self}, _build_function)
        #
        return tn

    def filter(self, filter_function):
        def _filter_function(serialized_value_any):
            value_any = self._deserialize_function(serialized_value_any)
            return filter_function(value_any)
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            i_in = self._output
            #
            i_2d = liftStreamIntroduction(g, evaluator, i_in)
            sel = LiftSelect(pred=_filter_function).connect(evaluator.circuit, (i_2d,))
            filtering = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (sel,))
            #
            tn._output = filtering
        #
        tn = TopologyNode("filter_op", {self}, _build_function)
        #
        return tn

    def join(self, other, predicate_function, projection_function):
        def _predicate_function(left_serialized_value_any, right_serialized_value_any):
            left_value_any = self._deserialize_function(left_serialized_value_any)
            right_value_any = self._deserialize_function(right_serialized_value_any)
            return predicate_function(left_value_any, right_value_any)
        #
        def _projection_function(left_serialized_value_any, right_serialized_value_any):
            left_value_any = self._deserialize_function(left_serialized_value_any)
            right_value_any = self._deserialize_function(right_serialized_value_any)
            return self._serialize_function(projection_function(left_value_any, right_value_any))
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            a_in = self._output
            b_in = other._output
            #
            a_2d = liftStreamIntroduction(g, evaluator, a_in)
            b_2d = liftStreamIntroduction(g, evaluator, b_in)
            join_ = DeltaLiftedDeltaLiftedJoin(
                pred=_predicate_function,
                proj=_projection_function,
                group_a=g,
                group_b=g,
                out_group=g,
            ).connect(evaluator.circuit, (a_2d, b_2d))
            #
            tn._output = join_
        #
        tn = TopologyNode("join_op", {self, other}, _build_function)
        #
        return tn

    def join_equi(self, other, left_select_function, right_select_function, projection_function):
        def _left_select_function(left_serialized_value_any):
            left_value_any = self._deserialize_function(left_serialized_value_any)
            return self._serialize_function(left_select_function(left_value_any))
        #
        def _right_select_function(right_serialized_value_any):
            right_value_any = self._deserialize_function(right_serialized_value_any)
            return self._serialize_function(right_select_function(right_value_any))
        #
        def _projection_function(_, left_serialized_value_any, right_serialized_value_any):
            left_value_any = self._deserialize_function(left_serialized_value_any)
            right_value_any = self._deserialize_function(right_serialized_value_any)
            return self._serialize_function(projection_function(left_value_any, right_value_any))
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            a_in = self._output
            b_in = other._output
            #
            emp_2d = liftStreamIntroduction(g, evaluator, a_in)
            sal_2d = liftStreamIntroduction(g, evaluator, b_in)
            emp_idx = LiftIndex[tuple[int, str], int](indexer=_left_select_function).connect(evaluator.circuit, (emp_2d,))
            sal_idx = LiftIndex[tuple[int, str], int](indexer=_right_select_function).connect(evaluator.circuit, (sal_2d,))
            smj = IndexedDeltaLiftedDeltaLiftedJoin[int, tuple[int, str], tuple[int, str], tuple[int, str, str]](
                proj=_projection_function,
                group_a=IndexedZSetAddition[int, tuple[int, str]](g, _left_select_function),
                group_b=IndexedZSetAddition[int, tuple[int, str]](g, _right_select_function),
                out_group=g,
            ).connect(evaluator.circuit, (emp_idx, sal_idx))
            #
            tn._output = smj
        #
        tn = TopologyNode("join_equi_op", {self, other}, _build_function)
        #
        return tn

    def group_by_agg(self, by_function, select_function, output_function, agg_function, agg_initial_any):
        def _by_function(serialized_value_any):
            value_any = self._deserialize_function(serialized_value_any)
            return by_function(value_any)
        #
        def _select_function(serialized_value_any):
            value_any = self._deserialize_function(serialized_value_any)
            return select_function(value_any)
        #
        def _output_function(key, sum_any):
            value_any = output_function(key, sum_any)
            return self._serialize_function(value_any)
        #
        def _build_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            i_in = self._output
            #
            cum = Integrate(group=g).connect(evaluator.circuit, (i_in,))
            agg = Lift1(f=zset_group_agg_function(_by_function, _select_function, agg_function, agg_initial_any, _output_function)).connect(evaluator.circuit, (cum,))
            agg_diffs = Differentiate(group=g).connect(evaluator.circuit, (agg,))
            #
            tn._output = agg_diffs
        #
        tn = TopologyNode("group_by_agg_op", {self}, _build_function)
        #
        return tn

    #

    def group_by_sum(self, by_function, select_function, output_function):
        tn = self.group_by_agg(by_function, select_function, output_function, lambda x, y, z: x + y * z, 0)
        tn._name = "group_by_sum_op"
        #
        return tn

    def group_by_max(self, by_function, select_function, output_function):
        tn = self.group_by_agg(by_function, select_function, output_function, lambda x, y, _: max(x, y), None)
        tn._name = "group_by_max_op"
        #
        return tn

    def group_by_min(self, by_function, select_function, output_function):
        tn = self.group_by_agg(by_function, select_function, output_function, lambda x, y, _: min(x, y), None)
        tn._name = "group_by_min_op"
        #
        return tn

    def group_by_count(self, by_function, output_function):
        tn = self.group_by_sum(by_function, lambda _: 1, output_function)
        tn._name = "group_by_count_op"
        #
        return tn

    #

    def agg(self, select_function, output_function, agg_function):
        tn = self.group_by_agg(lambda _: 0, select_function, output_function, agg_function)
        tn._name = "agg_op"
        #
        return tn

    def sum(self, select_function, output_function):
        tn = self.group_by_sum(lambda _: 0, select_function, output_function)
        tn._name = "sum_op"
        #
        return tn

    def max(self, select_function, output_function):
        tn = self.group_by_max(lambda _: 0, select_function, output_function)
        tn._name = "max_op"
        #
        return tn

    def min(self, select_function, output_function):
        tn = self.group_by_min(lambda _: 0, select_function, output_function)
        tn._name = "min_op"
        #
        return tn

    def count(self, output_function):
        tn = self.group_by_count(lambda _: 0, output_function)
        tn._name = "count_op"
        #
        return tn

    #

    def get_name(self):
        return self._name_str

    def get_id(self):
        return self._id_str

    def get_daughters(self):
        return self._daughter_tn_set

    def get_output(self):
        return self._output
    
    def get_evaluator(self):
        return self._evaluator

    #

    def latest(self, gc=True):
        gc_boolean = gc
        #
        zSet = self._evaluator.latest(self._output)
        # print(zSet)
        #
        if gc_boolean:
            self._evaluator.compact()
        #
        return zSet

    def push(self, source_str, message_dict_list):
        source_str_tn_dict = self.get_source_nodes()
        source_topologyNode = source_str_tn_dict[source_str]
        #
        input = source_topologyNode.get_output()
        #
        value_json_str_list = message_dict_list_to_value_json_str_list(message_dict_list)
        zSet = ZSet({value_json_str: 1 for value_json_str in value_json_str_list})
        #
        self._evaluator.push(input, zSet)

    def step(self, gc=True, bag=False):
        zSet = self.latest(gc)
        #
        updated_message_dict_list, deleted_message_dict_list = zSet_to_message_dict_list_tuple(zSet, bag)
        #
        return updated_message_dict_list, deleted_message_dict_list

    #

    def _foreach_bu(self, foreach_function):
        visited_tn_set = set()
        #
        def __foreach_bu(tn):
            if tn not in visited_tn_set:
                visited_tn_set.add(tn)
                #
                for daughter_tn in tn.get_daughters():
                    __foreach_bu(daughter_tn)
                #
                # print("---")
                # print(tn.get_name())
                # for d in tn.get_daughters():
                #     print(d.get_name())
                # print("---")
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
            for daughter_tn in tn.get_daughters():
                __filter_td(daughter_tn)
        #
        __filter_td(self)
        #
        return tn_set

    def get_node_by_id(self, id_str):
        tn_set = self._filter_td(lambda tn: tn.get_id() == id_str)
        #
        if len(tn_set) == 0:
            return None
        else:
            return list(tn_set)[0]
    
    def get_node_by_name(self, name_str):
        tn_set = self._filter_td(lambda tn: tn.get_name() == name_str)
        #
        if len(tn_set) == 0:
            return None
        else:
            return list(tn_set)[0]

    def get_source_nodes(self):
        tn_set = self._filter_td(lambda tn: len(tn.get_daughters()) == 0)
        #
        name_str_tn_dict = {tn.get_name(): tn for tn in tn_set}
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
                    return f"{self._name_str}_{self._id_str}({self._daughter_tn_set[0].topology(include_ids_bool)})"
                else:
                    return f"{self._name_str}({self._daughter_tn_set[0].topology(include_ids_bool)})"
            case 2:
                if include_ids_bool:
                    return  f"{self._name_str}_{self._id_str}({self._daughter_tn_set[0].topology(include_ids_bool)}, {self._daughter_tn_set[1].topology(include_ids_bool)})"
                else:
                    return  f"{self._name_str}({self._daughter_tn_set[0].topology(include_ids_bool)}, {self._daughter_tn_set[1].topology(include_ids_bool)})"

    def mermaid(self, include_ids=False):
        def collect_nodes_and_edges(tn, name_str_set, edge_str_list):
            if include_ids:
                name_str_set.add(f"{tn.get_name()}_{tn.get_id()}")
            else:
                name_str_set.add(tn.get_name())
            #
            for daughter_tn in tn.get_daughters():
                if include_ids:
                    edge_str_list.append(f"{daughter_tn.get_name()}_{daughter_tn.get_id()} --> {tn.get_name()}_{tn.get_id()}")
                else:
                    edge_str_list.append(f"{daughter_tn.get_name()} --> {tn.get_name()}")
                #
                collect_nodes_and_edges(daughter_tn, name_str_set, edge_str_list)
        #
        node_str_set = set()
        edge_str_list = []
        collect_nodes_and_edges(self, node_str_set, edge_str_list)
        #
        mermaid_str = ["graph TD"]
        mermaid_str.extend(f"    {name_str}" for name_str in node_str_set)
        mermaid_str.extend(f"    {edge_str}" for edge_str in edge_str_list)
        #
        return "\n".join(mermaid_str)

#

def json_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def message_dict_list_to_value_json_str_list(message_dict_list):
    value_json_str_list = []
    for message_dict in message_dict_list:
        value_dict = message_dict["value"]
        value_json_str = json.dumps(value_dict, default=json_default)
        value_json_str_list.append(value_json_str)
    #
    return value_json_str_list


def zSet_to_message_dict_list_tuple(zSet, bag_boolean=False):
    def value_json_str_to_message_dict(value_json_str):
        value_dict = json.loads(value_json_str)
        message_dict = {"value": value_dict}
        #
        return message_dict
    #
    updated_message_dict_list = []
    deleted_message_dict_list = []
    for value_json_str, weight_int in zSet.items():
        if weight_int > 0:
            if bag_boolean:
                for _ in range(weight_int):
                    message_dict = value_json_str_to_message_dict(value_json_str)
                    updated_message_dict_list.append(message_dict)
            else:
                message_dict = value_json_str_to_message_dict(value_json_str)
                updated_message_dict_list.append(message_dict)
        elif weight_int < 0:
            if bag_boolean:
                for _ in range(-weight_int):
                    message_dict = value_json_str_to_message_dict(value_json_str)
                    deleted_message_dict_list.append(message_dict)
            else:
                message_dict = value_json_str_to_message_dict(value_json_str)
                deleted_message_dict_list.append(message_dict)
    #
    return updated_message_dict_list, deleted_message_dict_list

#

def source(source_str):
    def _build_function(evaluator):
        tn._evaluator = evaluator
        #
        input = Input(frontier=Antichain(dbsp_time(1))).connect(evaluator.circuit, ())
        #
        tn._output = input
    #
    tn = TopologyNode(source_str, [], _build_function)
    #
    return tn


def zset_group_agg_function(_by_function, _select_function, _agg_function, agg_initial_any,_output_function):
    def _zset_group_agg_function(zSet):
        by_any_agg_any = {}
        #
        for value_json_str, weight_int in zSet.inner.items():
            by_any = _by_function(value_json_str)
            select_any = _select_function(value_json_str)
            #
            agg_any = by_any_agg_any.get(by_any, agg_initial_any)
            by_any_agg_any[by_any] = select_any if agg_any is None else _agg_function(agg_any, select_any, weight_int)
        #
        zSet = ZSet({_output_function(by_any, sum_any): 1 for by_any, sum_any in by_any_agg_any.items()})
        #
        return zSet
    #
    return _zset_group_agg_function

def liftStreamIntroduction(g, evaluator, i_in):
    return i_in if evaluator.frontiers()[i_in].lattice.nestedness == 2 else LiftStreamIntroduction(group=g).connect(evaluator.circuit, (i_in,))
