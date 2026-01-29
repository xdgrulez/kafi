from kafi.helpers import get_value, set_value

from pydbsp.stream import Stream, StreamHandle
from pydbsp.zset.operators.linear import LiftedProject
from pydbsp.stream.operators.linear import Differentiate, LiftedStreamIntroduction, LiftedStreamElimination
from pydbsp.stream import Stream, StreamHandle
from pydbsp.zset import ZSet, ZSetAddition

from kafi.streams.pydbsp_sql import CartesianProduct, Difference, Filtering, Intersection, Join, Selection, Union, GroupByThenAgg

import json
import time
import uuid

import cloudpickle as pickle

#

class TopologyNode:
    ###
    # Constructor
    ###
    
    def __init__(self, name_str, output_handle_function, step_function, gc_function, daughter_topologyNode_list=[], profile_dict=None):
        self._name_str = name_str
        self._id_str = str(uuid.uuid4())
        self._output_handle_function = output_handle_function
        self._step_function = step_function
        self._gc_function = gc_function
        self._daughter_topologyNode_list = daughter_topologyNode_list
        #
        self._group = output_handle_function().get().group()
        #
        self.init_profile_dict(profile_dict)

    #

    def init_profile_dict(self, profile_dict):
        if profile_dict is None:
            self._profile_dict = None
            return
        
        memory_unit_str_divisor_int_dict = {
            "KB": 1024,
            "MB": 1024 ** 2,
            "GB": 1024 ** 3,
            "TB": 1024 ** 4
        }
        
        memory_dict = profile_dict.get("memory", {})
        unit_str = memory_dict.get("unit", "KB").upper()
        
        self._profile_dict = {
            "time": profile_dict.get("time", False),
            "memory": {
                "before": memory_dict.get("before", False),
                "after": memory_dict.get("after", False),
                "delta": memory_dict.get("delta", False),
                "unit": unit_str,
                "divisor": memory_unit_str_divisor_int_dict.get(unit_str, 1024)
            },
            "include": profile_dict.get("include", [])
        }            
        
    #

    def map(self, map_function, profile_dict=None):
        def map_function1(value_json_str):
            value_dict = json.loads(value_json_str)
            return json.dumps(map_function(value_dict))
        #
        liftedStream = LiftedStreamIntroduction(self._output_handle_function())
        #
        selection = Selection(liftedStream.output_handle(), map_function1)
        #
        output_node = LiftedStreamElimination(selection.output_handle())
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            self.pydbsp_step(liftedStream)
            self.pydbsp_step(selection)
            self.pydbsp_step(output_node)
        #
        def gc_function():
            pass
        #
        topologyNode = TopologyNode("map_op", output_handle_function, step_function, gc_function, [self], profile_dict)
        return topologyNode

    def filter(self, filter_function, profile_dict=None):
        def filter_function1(value_json_str):
            value_dict = json.loads(value_json_str)
            return filter_function(value_dict)
        #
        liftedStream = LiftedStreamIntroduction(self._output_handle_function())
        #
        filtering = Filtering(liftedStream.output_handle(), filter_function1)
        #
        output_node = LiftedStreamElimination(filtering.output_handle())
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            topologyNode.pydbsp_step(liftedStream)
            topologyNode.pydbsp_step(filtering)
            topologyNode.pydbsp_step(output_node)
        #
        def gc_function():
            pass
        #
        topologyNode = TopologyNode("filter_op", output_handle_function, step_function, gc_function, [self], profile_dict)
        return topologyNode

    def join(self, other, on_function, projection_function, profile_dict=None):
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
            topologyNode.pydbsp_step(left_liftedStream)
            topologyNode.pydbsp_step(right_liftedStream)
            #
            topologyNode.pydbsp_step(join)
            #
            topologyNode.pydbsp_step(output_node)
        #
        def gc_function():
            left_liftedStream.gc()
            right_liftedStream.gc()
            #
            join.gc()
            #
            output_node.gc()
        #
        topologyNode = TopologyNode("join_op", output_handle_function, step_function, gc_function, [self, other], profile_dict)
        return topologyNode

    def union(self, other, profile_dict=None):
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
            topologyNode.pydbsp_step(left_liftedStream)
            topologyNode.pydbsp_step(right_liftedStream)
            #
            topologyNode.pydbsp_step(union)
            #
            topologyNode.pydbsp_step(output_node)
        #
        def gc_function():
            pass
        #
        topologyNode = TopologyNode("union_op", output_handle_function, step_function, gc_function, [self, other], profile_dict)
        return topologyNode

    def intersect(self, other, profile_dict=None):
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
            topologyNode.pydbsp_step(left_liftedStream)
            topologyNode.pydbsp_step(right_liftedStream)
            #
            topologyNode.pydbsp_step(intersection)
            #
            topologyNode.pydbsp_step(output_node)
        #
        def gc_function():
            pass
        #
        topologyNode = TopologyNode("intersect_op", output_handle_function, step_function, gc_function, [self, other], profile_dict)
        return topologyNode

    def difference(self, other, profile_dict=None):
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
            topologyNode.pydbsp_step(left_liftedStream)
            topologyNode.pydbsp_step(right_liftedStream)
            #
            topologyNode.pydbsp_step(difference)
            #
            topologyNode.pydbsp_step(output_node)
        #
        def gc_function():
            pass
        #
        topologyNode = TopologyNode("difference_op", output_handle_function, step_function, gc_function, [self, other], profile_dict)
        return topologyNode

    def product(self, other, profile_dict=None):
        left_liftedStream = LiftedStreamIntroduction(self._output_handle_function())
        right_liftedStream = LiftedStreamIntroduction(other._output_handle_function())
        #
        cartesianProduct = CartesianProduct(left_liftedStream.output_handle(), right_liftedStream.output_handle())
        #
        output_node = LiftedStreamElimination(cartesianProduct.output_handle())
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            topologyNode.pydbsp_step(left_liftedStream)
            topologyNode.pydbsp_step(right_liftedStream)
            #
            topologyNode.pydbsp_step(cartesianProduct)
            #
            topologyNode.pydbsp_step(output_node)
        #
        def gc_function():
            pass
        #
        topologyNode = TopologyNode("product_op", output_handle_function, step_function, gc_function, [self, other], profile_dict)
        return topologyNode
    
    def group_by_agg(self, by_function_list, as_function, agg_tuple_list, profile_dict=None):
        by_function = lambda value_dict: tuple(by_function(value_dict) for by_function in by_function_list)
        agg_select_function_agg_function_agg_as_function_tuple_list = agg_tuple_list
        agg_function = group_by_agg_fun([(agg_select_function, agg_function, agg_as_function, as_function) for agg_select_function, agg_function, agg_as_function in agg_select_function_agg_function_agg_as_function_tuple_list])
        #
        group_by_agg__topologyNode = self.group_by_agg_(by_function, agg_function, profile_dict=profile_dict)
        #
        return group_by_agg__topologyNode

    def group_by_agg_(self, by_function, agg_function, profile_dict=None):
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
            topologyNode.pydbsp_step(lifted_stream_introduction)
            #
            topologyNode.pydbsp_step(group_by_then_agg)
            #
            topologyNode.pydbsp_step(lifted_stream_elimination)
            #
            topologyNode.pydbsp_step(output_node)
        #
        def gc_function():
            lifted_stream_introduction.gc()
            #
            group_by_then_agg.gc()
            #
            lifted_stream_elimination.gc()
            #
            output_node.gc()
        #
        topologyNode = TopologyNode("group_by_agg_op", output_handle_function, step_function, gc_function, [self], profile_dict)
        return topologyNode 

    def agg(self, agg_tuple_list, profile_dict=None):
        agg_select_function_agg_function_agg_as_function_tuple_list = agg_tuple_list
        agg_function = group_by_agg_fun([(agg_select_function, agg_function, agg_as_function, None) for agg_select_function, agg_function, agg_as_function in agg_select_function_agg_function_agg_as_function_tuple_list])
        #
        group_by_agg__topologyNode = self.group_by_agg_(lambda _: None, agg_function, profile_dict=profile_dict)
        group_by_agg__topologyNode._name_str = "agg_op"
        #
        return group_by_agg__topologyNode

    def peek(self, peek_function, profile_dict=None):
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
        def gc_function():
            pass
        #
        topologyNode = TopologyNode("peek_op", output_handle_function, step_function, gc_function, [self], profile_dict)
        return topologyNode
    
    #

    def name(self):
        return self._name_str

    def id(self):
        return self._id_str

    def output_handle_function(self):
        return self._output_handle_function()

    def step_function(self):
        return self._step_function
    
    def gc_function(self):
        return self._gc_function

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
            #
            if topologyNode._profile_dict is not None:
                profile_before_tuple = topologyNode.profile_before(None, f"Profiling TopologyNode step {topologyNode._name_str}_{topologyNode._id_str}...")
            #
            topologyNode._step_function()
            #
            if topologyNode._profile_dict is not None:
                topologyNode.profile_after(profile_before_tuple, None, "...done.")
    
    def latest(self):
        return self._output_handle_function().get().latest()

    # def latest_until_fixpoint(self):
    #     latest_summed_zSet = ZSet({})
    #     latest_zSet = ZSet({})
    #     zSetAddition = ZSetAddition()
    #     while True:
    #         self.step()
    #         latest_zSet1 = self.latest()
    #         if latest_zSet1 != latest_zSet:
    #             if latest_zSet1.is_identity():
    #                 break
    #             latest_summed_zSet = zSetAddition.add(latest_summed_zSet, latest_zSet1)
    #             latest_zSet = latest_zSet1
    #         else:
    #             break
    #     return latest_summed_zSet

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

    def gc(self):
        self.gc_function()()
        #
        for topologyNode in self._daughter_topologyNode_list:
            topologyNode.gc()
    #
    
    # profile_dict: {"time": boolean, "memory": {"before": boolean, "after": boolean, "delta": boolean, "unit": str ("KB", "MB", "GB", "TB"), "divisor": int}, "include": operator_class_name_str_list}
    
    def pydbsp_step(self, pydbsp_operator):
        if self._profile_dict is not None:
            profile_before_tuple = self.profile_before(pydbsp_operator, f"  Profiling pyDBSP step {pydbsp_operator.__class__.__name__}...", "    ")
        #
        pydbsp_operator.step()
        #
        if self._profile_dict is not None:
            self.profile_after(profile_before_tuple, pydbsp_operator, "  ...done.", "    ")
            
    def profile_before(self, pydbsp_operator_object, before_str, indent_str=""):
        time_before_float = None
        memory_before_int = None
        #
        if self._profile_dict is not None:
            print_boolean = pydbsp_operator_object is None or pydbsp_operator_object.__class__.__name__ in self._profile_dict["include"]
            #
            if print_boolean:
                print(before_str)
            #
            if self._profile_dict["time"]:
                time_before_float = time.time()
            #
            if self._profile_dict["memory"]["before"] or self._profile_dict["memory"]["delta"]:
                memory_before_int = len(pickle.dumps(self)) / self._profile_dict["memory"]["divisor"]
                if self._profile_dict["memory"]["before"]:
                    if print_boolean:
                        print(f"{indent_str}Memory before: {memory_before_int} {self._profile_dict['memory']['unit']}")
        #
        profile_before_tuple = (time_before_float, memory_before_int)
        return profile_before_tuple

    def profile_after(self, profile_before_tuple, pydbsp_operator_object, after_str, indent_str=""):
        if self._profile_dict is not None:
            print_boolean = pydbsp_operator_object is None or pydbsp_operator_object.__class__.__name__ in self._profile_dict["include"]
            #
            time_before_float, memory_before_int = profile_before_tuple
            #
            if self._profile_dict["memory"]["after"] or self._profile_dict["memory"]["delta"]:
                memory_after_int = len(pickle.dumps(self)) / self._profile_dict["memory"]["divisor"]
                #
                if self._profile_dict["memory"]["after"]:
                    if print_boolean:
                        print(f"{indent_str}Memory after: {memory_after_int} {self._profile_dict['memory']['unit']}")
                if self._profile_dict["memory"]["delta"]:
                    if print_boolean:
                        print(f"{indent_str}Memory delta: {memory_after_int - memory_before_int} {self._profile_dict['memory']['unit']}")
            #
            if self._profile_dict["time"]:
                time_after_float = time.time()
                if print_boolean:
                    print(f"{indent_str}Time: {time_after_float - time_before_float}s")
            #
            if print_boolean:
                print(after_str)

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
                name_str_set.add(f"{topologyNode.name()}_{topologyNode.id()}")
            else:
                name_str_set.add(topologyNode.name())
            #
            for daughter_topologyNode in topologyNode.daughters():
                if include_ids:
                    edge_str_list.append(f"{daughter_topologyNode.name()}_{daughter_topologyNode.id()} --> {topologyNode.name()}_{topologyNode.id()}")
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


def sum(x, y):
    return x + y


def agg_tuple(select_function, agg_function, as_function):
    return (select_function, agg_function, as_function)


def group_by_agg_fun(agg_select_function_agg_function_agg_as_function_group_as_function_tuple_list):
    def group_by_agg_fun1(group_any_zset_tuple_zset):
        aggs_int = len(agg_select_function_agg_function_agg_as_function_group_as_function_tuple_list)
        #
        agg_group_any_group_any_agg_any_list_tuple_dict = {}
        for (group_any, zset), _ in group_any_zset_tuple_zset.items():
            agg_any_list = [None] * aggs_int
            #
            for value_str, weight_int in zset.items():
                value_dict = json.loads(value_str)
                #
                for i, (agg_select_function, agg_function, _, _) in enumerate(agg_select_function_agg_function_agg_as_function_group_as_function_tuple_list):
                    any = agg_select_function(value_dict)
                    #
                    if group_any not in agg_group_any_group_any_agg_any_list_tuple_dict:
                        agg_any_list[i] = any * weight_int
                    else:
                        agg_any_list[i] = agg_function(agg_any_list[i], any * weight_int)
                #
                agg_group_any_group_any_agg_any_list_tuple_dict[group_any] = (group_any, agg_any_list)
        #
        agg_value_str_weight_int_dict = {}
        for _, (group_any, agg_any_list) in agg_group_any_group_any_agg_any_list_tuple_dict.items():
            agg_value_dict = {}
            #
            for i, (_, _, agg_as_fun, group_as_fun) in enumerate(agg_select_function_agg_function_agg_as_function_group_as_function_tuple_list):                
                if group_as_fun is not None:
                    group_as_fun(agg_value_dict, group_any)
                #
                agg_as_fun(agg_value_dict, agg_any_list[i])
            #
            agg_value_str_weight_int_dict[json.dumps(agg_value_dict)] = 1
        #
        return ZSet(agg_value_str_weight_int_dict)
    #
    return group_by_agg_fun1

#

def source(name_str, profile_dict=None):
    stream = Stream(ZSetAddition())
    stream_handle = StreamHandle(lambda: stream)
    #
    def output_handle_function():
        return stream_handle
    #
    def step_function():
        pass
    #
    def gc_function():
        stream = stream_handle.get()
        current_time_int = stream.current_time()
        if current_time_int > 1:
            if current_time_int - 1 in stream.inner:
                del stream.inner[current_time_int - 1]
    #
    topologyNode = TopologyNode(name_str, output_handle_function, step_function, gc_function, [], profile_dict)
    return topologyNode


def message_dict_list_to_ZSet(message_dict_list):
    value_json_str_list = []
    for message_dict in message_dict_list:
        value_dict = message_dict["value"]
        # value_dict["_key"] = message_dict["key"]
        value_json_str = json.dumps(value_dict)
        value_json_str_list.append(value_json_str)
    #
    zSet = ZSet({k: 1 for k in value_json_str_list})
    return zSet
