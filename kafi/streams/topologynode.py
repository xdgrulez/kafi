from kafi.helpers import get_value, set_value

from pydbsp import (
    DeltaLiftedDeltaLiftedSortMergeJoin,
    LiftedLiftedProject,
    LiftedLiftedGroupBySum,
    DeltaLiftedDistinct,
    Program2D
)

import datetime
import json
# import ujson as json
import time
import uuid

import cloudpickle as pickle

#

class TopologyNode:
    def __init__(self, name_str, daughter_topologyNode_list, output_stream2D):
        self._name_str = name_str
        self._id_str = str(uuid.uuid4())
        self._daughter_topologyNode_list = daughter_topologyNode_list
        self._output_stream2D = output_stream2D
        #
        self._tick = 0

    #

    def map(self, map_function):
        def _map_function(value_json_str):
            value_dict = json.loads(value_json_str)
            return json.dumps(map_function(value_dict))
        #
        output_stream2D = LiftedLiftedProject(self._output_stream2D, _map_function)
        #
        topologyNode = TopologyNode("map_op", [self], output_stream2D)
        return topologyNode

    def join(self, other, left_on_function, right_on_function, projection_function):
        def _left_on_function(left_value_json_str):
            left_value_dict = json.loads(left_value_json_str)
            return json.dumps(left_on_function(left_value_dict))
        #
        def _right_on_function(right_value_json_str):
            right_value_dict = json.loads(right_value_json_str)
            return json.dumps(right_on_function(right_value_dict))
        #
        def _projection_function(left_value_json_str, right_value_json_str):
            left_value_dict = json.loads(left_value_json_str)
            right_value_dict = json.loads(right_value_json_str)
            return json.dumps(projection_function(left_value_dict, right_value_dict))
        #
        output_stream2D = DeltaLiftedDeltaLiftedSortMergeJoin(self._output_stream2D,
                                                              other._output_stream2D,
                                                              left_key=_left_on_function,
                                                              right_key=_right_on_function,
                                                              projection=_projection_function)
        #
        topologyNode = TopologyNode("join_op", [self, other], output_stream2D)
        return topologyNode

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
        output_stream2D = LiftedLiftedGroupBySum(self._output_stream2D, key=_by_function, value=_select_function, output=_output_function)
        #
        topologyNode = TopologyNode("group_by_sum_op", [self], output_stream2D)
        return topologyNode

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
        output_stream2D = LiftedLiftedGroupBySum(self._output_stream2D, key=lambda _: 0, value=_select_function, output=_output_function)
        #
        topologyNode = TopologyNode("sum_op", [self], output_stream2D)
        return topologyNode

    #

    def distinct(self):
        output_stream2D = DeltaLiftedDistinct(self._output_stream2D)
        #
        topologyNode = TopologyNode("distinct_op", [self], output_stream2D)
        return topologyNode
    #

    def get_name(self):
        return self._name_str

    def get_id(self):
        return self._id_str

    def get_daughters(self):
        return self._daughter_topologyNode_list

    def get_output_stream2D(self):
        return self._output_stream2D
    
    #

    def get_node_by_id(self, id_str):
        def collect_node_dict(topologyNode, id_str_topologyNode_dict):
            if id_str == topologyNode.get_id():
                id_str_topologyNode_dict[id_str] = topologyNode
            else:
                for daughter_topologyNode in topologyNode.get_daughters():
                    collect_node_dict(daughter_topologyNode, id_str_topologyNode_dict)
        #
        id_str_topologyNode_dict = {}
        collect_node_dict(self, id_str_topologyNode_dict)
        #
        return id_str_topologyNode_dict[id_str]

    def get_node_by_name(self, name_str):
        def collect_node_dict(topologyNode, name_str_topologyNode_dict):
            if name_str == topologyNode.get_name():
                name_str_topologyNode_dict[name_str] = topologyNode
            else:
                for daughter_topologyNode in topologyNode.get_daughters():
                    collect_node_dict(daughter_topologyNode, name_str_topologyNode_dict)
        #
        name_str_topologyNode_dict = {}
        collect_node_dict(self, name_str_topologyNode_dict)
        #
        return name_str_topologyNode_dict[name_str]

    def get_source_nodes(self):
        def collect_node_dict(topologyNode, name_str_topologyNode_dict):
            if not topologyNode.get_daughters():
                name_str_topologyNode_dict[topologyNode.get_name()] = topologyNode
            else:
                for daughter_topologyNode in topologyNode.get_daughters():
                    collect_node_dict(daughter_topologyNode, name_str_topologyNode_dict)
        #
        name_str_topologyNode_dict = {}
        collect_node_dict(self, name_str_topologyNode_dict)
        #
        return name_str_topologyNode_dict

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
                    return f"{self._name_str}_{self._id_str}({self._daughter_topologyNode_list[0].topology(include_ids_bool)})"
                else:
                    return f"{self._name_str}({self._daughter_topologyNode_list[0].topology(include_ids_bool)})"
            case 2:
                if include_ids_bool:
                    return  f"{self._name_str}_{self._id_str}({self._daughter_topologyNode_list[0].topology(include_ids_bool)}, {self._daughter_topologyNode_list[1].topology(include_ids_bool)})"
                else:
                    return  f"{self._name_str}({self._daughter_topologyNode_list[0].topology(include_ids_bool)}, {self._daughter_topologyNode_list[1].topology(include_ids_bool)})"

    def mermaid(self, include_ids=False):
        def collect_nodes_and_edges(topologyNode, name_str_set, edge_str_list):
            if include_ids:
                name_str_set.add(f"{topologyNode.get_name()}_{topologyNode.get_id()}")
            else:
                name_str_set.add(topologyNode.get_name())
            #
            for daughter_topologyNode in topologyNode.get_daughters():
                if include_ids:
                    edge_str_list.append(f"{daughter_topologyNode.get_name()}_{daughter_topologyNode.get_id()} --> {topologyNode.get_name()}_{topologyNode.get_id()}")
                else:
                    edge_str_list.append(f"{daughter_topologyNode.get_name()} --> {topologyNode.get_name()}")
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

class Runner():
    def __init__(self, gc=True, parallelism=1, parallel_layer_min_width=4):
        self._program2D = Program2D(gc=gc, parallelism=parallelism, parallel_layer_min_width=parallel_layer_min_width)

    def source(self, source_str):
        output_stream2D = self._program2D.source(source_str)
        #
        topologyNode = TopologyNode(source_str, [], output_stream2D)
        #
        return topologyNode
    
    def root(self, root_topologyNode):
        self._root_topologyNode = root_topologyNode
        #
        self._view = self._program2D.view("root", root_topologyNode.get_output_stream2D())

    def step(self, bag=False):
        bag_boolean = bag
        #
        zSet = self._program2D.step()[self._view]
        #
        updated_message_dict_list, deleted_message_dict_list = zSet_to_message_dict_list_tuple(zSet, bag_boolean)
        #
        return updated_message_dict_list, deleted_message_dict_list
    
    def insert(self, source_str, message_dict_list):
        source_topologyNode = self._root_topologyNode.get_node_by_name(source_str)
        value_json_str_list = message_dict_list_to_value_json_str_list(message_dict_list)
        #
        self._program2D.insert(source_topologyNode.get_output_stream2D(), value_json_str_list)

    def get_program2D(self):
        return self._program2D

    def get_root(self):
        return self._root_topologyNode
    
    def get_view(self):
        return self._view

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
                updated_message_dict_list.append(message_dict)
        else:
            raise Exception(f"ZSet elements with weight 0 are not supported: {value_json_str}, {weight_int}")
    #
    return updated_message_dict_list, deleted_message_dict_list

#

def traverse(topologyNode, stack_topologyNode_list):
    stack_topologyNode_list.append(topologyNode)
    for daughter_topologyNode in topologyNode.get_daughters():
        traverse(daughter_topologyNode, stack_topologyNode_list)
    return stack_topologyNode_list
