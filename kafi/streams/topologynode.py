from kafi.helpers import get_value, set_value

from pydbsp.circuit import Circuit
from pydbsp.compute import ComputeCtx
from pydbsp.core import Antichain, dbsp_time
from pydbsp.evaluate import Evaluator
from pydbsp.operator import Input, Lift1, LiftStreamIntroduction
from pydbsp.relational_operators import (
    DeltaLiftedDeltaLiftedDistinct,
    DeltaLiftedDeltaLiftedJoin,
    LiftProject,
    LiftSelect,
)
from pydbsp.operator import Differentiate, Integrate
from pydbsp.storage import DictStorage
from pydbsp.zset import ZSet, ZSetAddition

from collections import deque
import datetime
import json
# import ujson as json
import uuid

#

class TopologyNode:
    def __init__(self, name_str, daughter_tn_list, setup_function):
        self._name_str = name_str
        self._id_str = str(uuid.uuid4())
        self._daughter_tn_list = daughter_tn_list
        self._setup_function = setup_function
        #
        self._evaluator = None
        self._output = None

    def setup(self):
        evaluator = Evaluator(
            circuit=Circuit(),
            storage=DictStorage(),
            ctx=ComputeCtx(lattice=dbsp_time(2)),
            group=ZSetAddition())
        #
        self._foreach_bu(lambda tn: tn._setup_function(evaluator))

    #

    def map(self, map_function):
        def _map_function(value_json_str):
            value_dict = json.loads(value_json_str)
            return json.dumps(map_function(value_dict))
        #
        def _setup_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            i_in = self._output
            #
            i_2d = LiftStreamIntroduction(group=g).connect(evaluator.circuit, (i_in,))
            proj = LiftProject(f=_map_function).connect(evaluator.circuit, (i_2d,))
            selection = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (proj,))
            #
            tn._output = selection
        #
        tn = TopologyNode("map_op", [self], _setup_function)
        #
        return tn

    def filter(self, filter_function):
        def _filter_function(value_json_str):
            value_dict = json.loads(value_json_str)
            return filter_function(value_dict)
        #
        def _setup_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            i_in = self._output
            #
            i_2d = LiftStreamIntroduction(group=g).connect(evaluator.circuit, (i_in,))
            sel = LiftSelect(pred=_filter_function).connect(evaluator.circuit, (i_2d,))
            filtering = DeltaLiftedDeltaLiftedDistinct(inner_group=g).connect(evaluator.circuit, (sel,))
            #
            tn._output = filtering
        #
        tn = TopologyNode("filter_op", [self], _setup_function)
        #
        return tn

    def join(self, other, predicate_function, projection_function):
        def _predicate_function(left_value_json_str, right_value_json_str):
            left_value_dict = json.loads(left_value_json_str)
            right_value_dict = json.loads(right_value_json_str)
            return predicate_function(left_value_dict, right_value_dict)
        #
        def _projection_function(left_value_json_str, right_value_json_str):
            left_value_dict = json.loads(left_value_json_str)
            right_value_dict = json.loads(right_value_json_str)
            return json.dumps(projection_function(left_value_dict, right_value_dict))
        #
        def _setup_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            a_in = self._output
            b_in = other._output
            #
            a_2d = LiftStreamIntroduction(group=g).connect(evaluator.circuit, (a_in,))
            b_2d = LiftStreamIntroduction(group=g).connect(evaluator.circuit, (b_in,))
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
        tn = TopologyNode("join_op", [self, other], _setup_function)
        #
        return tn

    def group_by_sum(self, by_function, select_function, output_function):
        def _by_function(value_json_str):
            value_dict = json.loads(value_json_str)
            return by_function(value_dict)
        #
        def _select_function(value_json_str):
            value_dict = json.loads(value_json_str)
            return select_function(value_dict)
        #
        def _output_function(key, sum):
            value_dict = output_function(key, sum)
            value_json_str = json.dumps(value_dict)
            return value_json_str
        #
        def _setup_function(evaluator):
            tn._evaluator = evaluator
            #
            g = ZSetAddition()
            #
            i_in = self._output
            #
            cum = Integrate(group=g).connect(evaluator.circuit, (i_in,))
            agg = Lift1(f=zset_group_sum(_by_function, _select_function, _output_function)).connect(evaluator.circuit, (cum,))
            agg_diffs = Differentiate(group=g).connect(evaluator.circuit, (agg,))
            #
            tn._output = agg_diffs
        #
        tn = TopologyNode("group_by_sum_op", [self], _setup_function)
        #
        return tn

    #

    def sum(self, select_function, output_function):
        def _select_function(value_json_str):
            value_dict = json.loads(value_json_str)
            return select_function(value_dict)
        #
        def _output_function(key, sum):
            value_dict = output_function(key, sum)
            value_json_str = json.dumps(value_dict)
            return value_json_str
        #
        def _setup_function(evaluator):
            tn._evaluator = evaluator
            #
            g_in = ZSetAddition[tuple[str, int]]()
            g_out = ZSetAddition[tuple[str, int]]()
            #
            i_in = self._output
            #
            cum = Integrate[ZSet[tuple[str, int]]](group=g_in).connect(evaluator.circuit, (i_in,))
            agg = Lift1[ZSet[tuple[str, int]], ZSet[tuple[str, int]]](f=zset_group_sum(lambda _: 0, _select_function, _output_function)).connect(evaluator.circuit, (cum,))
            agg_diffs = Differentiate[ZSet[tuple[str, int]]](group=g_out).connect(evaluator.circuit, (agg,))
            #
            tn._output = agg_diffs
        #
        tn = TopologyNode("sum_op", [self], _setup_function)
        #
        return tn

    #

    def get_name(self):
        return self._name_str

    def get_id(self):
        return self._id_str

    def get_daughters(self):
        return self._daughter_tn_list

    def get_output(self):
        return self._output
    
    def get_evaluator(self):
        return self._evaluator

    #


    def _get_topology(self):
        levels = [[]]
        # Initialize the queue with the root node and a level marker (None).
        double_ended_queue = deque([self, None])
        #
        while double_ended_queue:
            tn = double_ended_queue.popleft()
            #
            if tn is None:
                # Marker found => this level of the tree is finished.
                if double_ended_queue:
                    levels.append([])
                    # Add marker for the next level.
                    double_ended_queue.append(None)
            else:
                # Node found => add into current level.
                levels[-1].append(tn)
                # Append daughters to the end of the queue.
                for daughter in tn.get_daughters():
                    double_ended_queue.append(daughter)
        #                    
        return levels

    def _collect_bu(self):
        levels = self._get_topology()
        tn_list = sum(levels, [])        
        tn_list.reverse()
        #
        return tn_list

    def _foreach_bu(self, foreach_function):
        tn_list = self._collect_bu()
        #
        for tn in tn_list:
            foreach_function(tn)

    #

    def latest(self, gc=True):
        gc_boolean = gc
        #
        zSet = self._evaluator.latest(self._output)
        #
        if gc_boolean:
            self._evaluator.compact()
        #
        return zSet

    def push(self, source_str, message_dict_list):
        source_str_tn_dict = self.get_source_nodes()
        source_topologyNode = source_str_tn_dict[source_str]
        input = source_topologyNode.get_output()
        value_json_str_list = message_dict_list_to_value_json_str_list(message_dict_list)
        #
        for value_json_str in value_json_str_list:
            zSet = ZSet({value_json_str: 1})
            self._evaluator.push(input, zSet)

    def step(self, gc=True):
        zSet = self.latest(gc)
        #
        updated_message_dict_list, deleted_message_dict_list = zSet_to_message_dict_list_tuple(zSet)
        #
        return updated_message_dict_list, deleted_message_dict_list

    #

    def _filter_bu(self, filter_function):
        tn_list = self._collect_bu()
        #
        filtered_tn_list = [tn for tn in tn_list if filter_function(tn)]
        #
        return filtered_tn_list

    def get_node_by_id(self, id_str):
        tn_list = self._filter_bu(lambda tn: tn.get_id() == id_str)
        #
        if len(tn_list) == 0:
            return None
        else:
            return tn_list[0]
    
    def get_node_by_name(self, name_str):
        tn_list = self._filter_bu(lambda tn: tn.get_name() == name_str)
        #
        if len(tn_list) == 0:
            return None
        else:
            return tn_list[0]

    def get_source_nodes(self):
        tn_set = self._filter_bu(lambda tn: len(tn.get_daughters()) == 0)
        #
        name_str_tn_dict = {tn.get_name(): tn for tn in tn_set}
        #
        return name_str_tn_dict

    #

    def topology(self, include_ids=False):
        include_ids_bool = include_ids
        #
        daughters_int = len(self._daughter_tn_list)
        match daughters_int:
            case 0:
                if include_ids_bool:
                    return f"{self._name_str}_{self._id_str}"
                else:
                    return self._name_str
            case 1:
                if include_ids_bool:
                    return f"{self._name_str}_{self._id_str}({self._daughter_tn_list[0].topology(include_ids_bool)})"
                else:
                    return f"{self._name_str}({self._daughter_tn_list[0].topology(include_ids_bool)})"
            case 2:
                if include_ids_bool:
                    return  f"{self._name_str}_{self._id_str}({self._daughter_tn_list[0].topology(include_ids_bool)}, {self._daughter_tn_list[1].topology(include_ids_bool)})"
                else:
                    return  f"{self._name_str}({self._daughter_tn_list[0].topology(include_ids_bool)}, {self._daughter_tn_list[1].topology(include_ids_bool)})"

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

def get(*key_str_tuple):
    def get1(value_dict):
        return get_value(value_dict, list(key_str_tuple))
    #
    return get1


def update(*key_str_tuple):
    def update1(value_dict, any):
        set_value(value_dict, list(key_str_tuple), any)
        return value_dict
    #
    return update1

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
    def _setup_function(evaluator):
        tn._evaluator = evaluator
        #
        input = Input(frontier=Antichain(dbsp_time(1))).connect(evaluator.circuit, ())
        #
        tn._output = input
    #
    tn = TopologyNode(source_str, [], _setup_function)
    #
    return tn


def zset_group_sum(_by_function, _select_function, _output_function):
    def _zset_group_sum(zSet):
        by_any_sum_any = {}
        #
        for value_json_str, weight_int in zSet.inner.items():
            by_any = _by_function(value_json_str)
            select_any = _select_function(value_json_str)
            #
            by_any_sum_any[by_any] = by_any_sum_any.get(by_any, 0) + select_any * weight_int
        #
        zSet = ZSet({_output_function(by_any, sum_any): 1 for by_any, sum_any in by_any_sum_any.items()})
        #
        return zSet
    #
    return _zset_group_sum

def _get_topology(tn):
    graph = {}
        
    # Falls der Knoten noch nicht im Graph ist, füge ihn hinzu
    if tn not in graph:
        graph[tn] = []
        
    # Gehe durch alle Töchter
    for daughter in tn.get_daughters():
        # Trage die Gegenrichtung ein: Tochter zeigt auf diesen Eltern-Knoten
        if daughter not in graph:
            graph[daughter] = []
        graph[daughter].append(tn)
        
        # Rekursiver Aufruf für die Tochter
        _get_topology(daughter, graph)
        
    return graph
