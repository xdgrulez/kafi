from kafi.helpers import get_value, set_value

from pydbsp.stream import step_until_fixpoint, Stream, StreamHandle
from pydbsp.zset.operators.linear import LiftedSelect, LiftedProject
from pydbsp.stream.operators.linear import Differentiate, LiftedStreamIntroduction, LiftedStreamElimination
from pydbsp.stream import Stream, StreamHandle
from pydbsp.zset import ZSet, ZSetAddition

from kafi.streams.pydbsp_sql import Difference, Intersection, Join, Union, GroupByThenAgg

import json
import uuid

#

class TopologyNode:
    def __init__(self, name_str, output_handle_function=lambda: None, step_function=lambda: None, daughter_topologyNode_list=[]):
        self._name_str = name_str
        self._id_str = str(uuid.uuid4())
        self._output_handle_function = output_handle_function
        self._step_function = step_function
        self._daughter_topologyNode_list = daughter_topologyNode_list
        #
        self.group = output_handle_function().get().group()

    def map(self, map_function):
        def map_function1(value_json_str):
            value_dict = json.loads(value_json_str)
            return json.dumps(map_function(value_dict))
        #
        liftedProject = LiftedProject(self._output_handle_function(), map_function1)
        #
        def output_handle_function():
            return liftedProject.output_handle()
        #
        def step_function():
            liftedProject.step()
        #
        return TopologyNode("map_op", output_handle_function, step_function, [self])

    def filter(self, filter_function):
        def filter_function1(value_json_str):
            value_dict = json.loads(value_json_str)
            return filter_function(value_dict)
        #
        liftedSelect = LiftedSelect(self._output_handle_function(), filter_function1)
        #
        def output_handle_function():
            return liftedSelect.output_handle()
        #
        def step_function():
            liftedSelect.step()
        #
        return TopologyNode("filter_op", output_handle_function, step_function, [self])

    def join(self, other, on_function, projection_function):
        def on_function1(left_value_json_str, right_value_json_str):
            left_value_dict = json.loads(left_value_json_str)
            right_value_dict = json.loads(right_value_json_str)
            return on_function(left_value_dict, right_value_dict)
        #
        def projection_function1(left_value_json_str, right_value_json_str):
            left_value_dict = json.loads(left_value_json_str)
            right_value_dict = json.loads(right_value_json_str)
            return json.dumps(projection_function(left_value_dict, right_value_dict))
        #
        left_liftedStream = LiftedStreamIntroduction(self._output_handle_function())
        right_liftedStream = LiftedStreamIntroduction(other._output_handle_function())
        #
        join = Join(left_liftedStream.output_handle(), right_liftedStream.output_handle(), on_function1, projection_function1)
        #
        output_node = LiftedStreamElimination(join.output_handle())
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            left_liftedStream.step()
            right_liftedStream.step()
            #
            join.step()
            #
            output_node.step()
        #
        return TopologyNode("join_op", output_handle_function, step_function, [self, other])

    def union(self, other):
        left_liftedStream = LiftedStreamIntroduction(self._output_handle_function())
        right_liftedStream = LiftedStreamIntroduction(other._output_handle_function())
        #
        union = Union(left_liftedStream.output_handle(), right_liftedStream.output_handle())
        #
        output_node = LiftedStreamElimination(union.output_handle())
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            left_liftedStream.step()
            right_liftedStream.step()
            #
            union.step()
            #
            output_node.step()
        #
        return TopologyNode("union_op", output_handle_function, step_function, [self, other])

    def intersect(self, other):
        left_liftedStream = LiftedStreamIntroduction(self._output_handle_function())
        right_liftedStream = LiftedStreamIntroduction(other._output_handle_function())
        #
        intersection = Intersection(left_liftedStream.output_handle(), right_liftedStream.output_handle())
        #
        output_node = LiftedStreamElimination(intersection.output_handle())
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            left_liftedStream.step()
            right_liftedStream.step()
            #
            intersection.step()
            #
            output_node.step()
        #
        return TopologyNode("intersect_op", output_handle_function, step_function, [self, other])

    def difference(self, other):
        left_liftedStream = LiftedStreamIntroduction(self._output_handle_function())
        right_liftedStream = LiftedStreamIntroduction(other._output_handle_function())
        #
        difference = Difference(left_liftedStream.output_handle(), right_liftedStream.output_handle())
        #
        output_node = LiftedStreamElimination(difference.output_handle())
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            left_liftedStream.step()
            right_liftedStream.step()
            #
            difference.step()
            #
            output_node.step()
        #
        return TopologyNode("difference_op", output_handle_function, step_function, [self, other])

    def group_by_agg(self, by_function, agg_function):
        def by_function1(value_json_str):
            value_dict = json.loads(value_json_str)
            return by_function(value_dict)
        #
        def agg_function1(group_any_zset_tuple_zset):
            return agg_function(group_any_zset_tuple_zset)
        #
        stream_handle = self._output_handle_function()
        lifted_stream_introduction = LiftedStreamIntroduction(stream_handle)
        group_by_then_agg = GroupByThenAgg(lifted_stream_introduction.output_handle(), by_function1, agg_function1)
        lifted_stream_elimination = LiftedStreamElimination(group_by_then_agg.output_handle())
        output_node = Differentiate(lifted_stream_elimination.output_handle())
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            lifted_stream_introduction.step()
            group_by_then_agg.step()
            #
            lifted_stream_elimination.step()
            output_node.step()
        #
        return TopologyNode("group_by_agg_op", output_handle_function, step_function, [self])

    def agg(self, agg_function):
        def by_function1(_):
            return None
        #
        def agg_function1(group_any_zset_tuple_zset):
            return agg_function(group_any_zset_tuple_zset)
        #
        stream_handle = self._output_handle_function()
        lifted_stream_introduction = LiftedStreamIntroduction(stream_handle)
        group_by_then_agg = GroupByThenAgg(lifted_stream_introduction.output_handle(), by_function1, agg_function1)
        lifted_stream_elimination = LiftedStreamElimination(group_by_then_agg.output_handle())
        output_node = Differentiate(lifted_stream_elimination.output_handle())
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            lifted_stream_introduction.step()
            group_by_then_agg.step()
            #
            lifted_stream_elimination.step()
            output_node.step()
        #
        return TopologyNode("agg_op", output_handle_function, step_function, [self])

    def peek(self, peek_function):
        def peek_function1(value_json_str):
            value_dict = json.loads(value_json_str)
            peek_function(value_dict)
            return value_json_str
        #
        liftedProject = LiftedProject(self._output_handle_function(), peek_function1)
        #
        def output_handle_function():
            return liftedProject.output_handle()
        #
        def step_function():
            liftedProject.step()
        #
        return TopologyNode("peek_op", output_handle_function, step_function, [self])
    
    #

    def name(self):
        return self._name_str

    def id(self):
        return self._id_str

    def output_handle_function(self):
        return self._output_handle_function

    def step_function(self):
        return self._step_function

    def daughters(self):
        return self._daughter_topologyNode_list    

    #

    def step(self):
        def traverse(topologyNode, stack):
            stack.append(topologyNode)
            for daughter in topologyNode.daughters():
                traverse(daughter, stack)
            return stack
        #
        stack = traverse(self, [])
        #
        while stack:
            topologyNode = stack.pop()
            topologyNode._step_function()
    
    def latest(self):
        return self._output_handle_function().get().latest()

    def latest_until_fixpoint(self):
        latest_summed_zSet = ZSet({})
        latest_zSet = ZSet({})
        zSetAddition = ZSetAddition()
        while True:
            self.step()
            latest_zSet1 = self.latest()
            if latest_zSet1 != latest_zSet:
                if latest_zSet1.is_identity():
                    break
                latest_summed_zSet = zSetAddition.add(latest_summed_zSet, latest_zSet1)
                latest_zSet = latest_zSet1
            else:
                break
        return latest_summed_zSet

    #

    def get_node_by_id(self, id_str):
        def collect_node_dict(topologyNode, id_str_topologyNode_dict):
            if id_str == topologyNode.id():
                id_str_topologyNode_dict[id_str] = topologyNode
            else:
                for daughter_topologyNode in topologyNode.daughters():
                    collect_node_dict(daughter_topologyNode, id_str_topologyNode_dict)
        #
        id_str_topologyNode_dict = {}
        collect_node_dict(self, id_str_topologyNode_dict)
        #
        return id_str_topologyNode_dict[id_str]

    def get_node_by_name(self, name_str):
        def collect_node_dict(topologyNode, name_str_topologyNode_dict):
            if name_str == topologyNode.name():
                name_str_topologyNode_dict[name_str] = topologyNode
            else:
                for daughter_topologyNode in topologyNode.daughters():
                    collect_node_dict(daughter_topologyNode, name_str_topologyNode_dict)
        #
        name_str_topologyNode_dict = {}
        collect_node_dict(self, name_str_topologyNode_dict)
        #
        return name_str_topologyNode_dict[name_str]

    #

    def topology(self, include_ids=False):
        include_ids_bool = include_ids
        #
        daughters_int = len(self._daughter_topologyNode_list)
        match daughters_int:
            case 0:
                if include_ids_bool:
                    return f"{self._name_str}_{self._id_str}"
                else:
                    return self._name_str
            case 1:
                if include_ids_bool:
                    return f"{self._name_str}_{self._id_str}({self._daughter_topologyNode_list[0].topology()})"
                else:
                    return f"{self._name_str}({self._daughter_topologyNode_list[0].topology()})"
            case 2:
                if include_ids_bool:
                    return  f"{self._name_str}_{self._id_str}({self._daughter_topologyNode_list[0].topology()}, {self._daughter_topologyNode_list[1].topology()})"
                else:
                    return  f"{self._name_str}({self._daughter_topologyNode_list[0].topology()}, {self._daughter_topologyNode_list[1].topology()})"

    def mermaid(self, include_ids=False):
        def collect_nodes_and_edges(topologyNode, name_str_set, edge_str_list):
            if include_ids:
                name_str_set.add(f"{topologyNode.name()}_{topologyNode.id()}")
            else:
                name_str_set.add(topologyNode.name())
            #
            for daughter_topologyNode in topologyNode.daughters():
                if include_ids:
                    edge_str_list.append(f"{daughter_topologyNode.name()}_{topologyNode.id()} --> {topologyNode.name()}_{topologyNode.id()}")
                else:
                    edge_str_list.append(f"{daughter_topologyNode.name()} --> {topologyNode.name()}")
                #
                collect_nodes_and_edges(daughter_topologyNode, name_str_set, edge_str_list)
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

def select_fun(key_str_list):
    def select_fun1(value_dict):
        return get_value(value_dict, key_str_list)
    #
    return select_fun1


def as_fun(key_str_list):
    def as_fun1(value_dict, any):
        set_value(value_dict, key_str_list, any)
        return value_dict
    #
    return as_fun1


def select_as_fun(key_str_list):
    def select_as_fun1(value_dict, any=None):
        if any is not None:
            set_value(value_dict, key_str_list, any)
            return value_dict
        else:
            return get_value(value_dict, key_str_list)
    #
    return select_as_fun1


def agg_fun(agg_fun_select_fun_agg_select_as_fun_tuple_list):
    agg_fun_select_fun_agg_select_as_fun_group_as_fun_tuple_list = [(agg_fun, select_fun, agg_select_as_fun, None) for agg_fun, select_fun, agg_select_as_fun in agg_fun_select_fun_agg_select_as_fun_tuple_list]
    #
    return group_by_agg_fun(agg_fun_select_fun_agg_select_as_fun_group_as_fun_tuple_list)


def group_by_agg_fun(agg_fun_select_fun_agg_select_as_fun_group_as_fun_tuple_list):
    def group_by_agg_fun1(group_any_zset_tuple_zset):
        agg_group_any_value_str_dict = {}
        for agg_fun, select_fun, agg_select_as_fun, group_as_fun in agg_fun_select_fun_agg_select_as_fun_group_as_fun_tuple_list:
            for (group_any, zset), _ in group_any_zset_tuple_zset.items():
                for value_str, weight_int in zset.items():
                    value_dict = json.loads(value_str)
                    if group_any not in agg_group_any_value_str_dict:
                        any = select_fun(value_dict)
                        value_dict1 = {}
                        value_dict1 = agg_select_as_fun(value_dict1, any * weight_int)
                    else:
                        any = select_fun(value_dict)
                        value_dict1 = json.loads(agg_group_any_value_str_dict[group_any])
                        any1 = agg_select_as_fun(value_dict1)
                        value_dict1 = agg_select_as_fun(value_dict1, agg_fun(any1, any * weight_int))
                    #
                    if group_any is not None:
                        value_dict1 = group_as_fun(value_dict1, group_any)
                    agg_group_any_value_str_dict[group_any] = json.dumps(value_dict1)
        #
        return ZSet({value_str: 1 for _, value_str in agg_group_any_value_str_dict.items()})
    #
    return group_by_agg_fun1

#

def source(name_str):
    stream = Stream(ZSetAddition())
    stream_handle = StreamHandle(lambda: stream)
    #
    def output_handle_function():
        return stream_handle
    #
    return TopologyNode(name_str, output_handle_function)


def message_dict_list_to_ZSet(message_dict_list):
    value_json_str_list = [json.dumps(message_dict["value"]) for message_dict in message_dict_list]
    zSet = ZSet({k: 1 for k in value_json_str_list})
    return zSet
