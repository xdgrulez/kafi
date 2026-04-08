from kafi.helpers import get_value, set_value

from pydbsp.stream import Stream, StreamHandle
from pydbsp.zset.operators.linear import LiftedProject, LiftedSelect
from pydbsp.stream.operators.linear import Differentiate, LiftedStreamIntroduction, LiftedStreamElimination
from pydbsp.stream import Stream, StreamHandle
from pydbsp.zset import ZSet, ZSetAddition
from pydbsp.indexed_zset.operators.linear import LiftedIndex, LiftedLiftedIndex
from pydbsp.indexed_zset.operators.bilinear import DeltaLiftedDeltaLiftedSortMergeJoin
from pydbsp.indexed_zset.functions.bilinear import join_with_index
from pydbsp.stream.operators.bilinear import Incrementalize2

from kafi.streams.pydbsp_sql import CartesianProduct, Difference, Filtering, Intersection, Join, Selection, Union, GroupByThenAgg

import datetime
import json
# import ujson as json
import time
import uuid

import cloudpickle as pickle

#

class TopologyNode:
    ###
    # Constructor
    ###
    
    def __init__(self, name_str, output_handle_function, step_function, gc_function, daughter_topologyNode_list=[], profile_config_dict=None):
        self._name_str = name_str
        self._id_str = str(uuid.uuid4())
        self._output_handle_function = output_handle_function
        self._step_function = step_function
        self._gc_function = gc_function
        self._daughter_topologyNode_list = daughter_topologyNode_list
        #
        self._group = output_handle_function().get().group()
        #
        self._counter_int = 0
        #
        self._profile_config_dict = self.profile_config(profile_config_dict)
        #
        self._profile_dict = {}

    #

    def profile_config(self, profile_config_dict):
        if profile_config_dict is None:
            return None
        #
        _profile_config_dict = {}
        for step_or_gc_str in ["step", "gc"]:
            profile_config_dict1 = profile_config_dict.get(step_or_gc_str, {})
            memory_dict = profile_config_dict1.get("memory", {})
            #
            _profile_config_dict1 = {
                "time": profile_config_dict1.get("time", False),
                "memory": {
                    "before": memory_dict.get("before", False),
                    "after": memory_dict.get("after", False),
                    "delta": memory_dict.get("delta", False),
                },
                "streams": profile_config_dict1.get("streams", None)
            }
            #
            _profile_config_dict[step_or_gc_str] = _profile_config_dict1
        #
        memory_unit_str_divisor_int_dict = {
            "KB": 1024,
            "MB": 1024 ** 2,
            "GB": 1024 ** 3,
            "TB": 1024 ** 4
        }
        unit_str = memory_dict.get("unit", "KB").upper()
        divisor_int = memory_unit_str_divisor_int_dict.get(unit_str, 1024)
        #
        _profile_config_dict["unit"] = unit_str
        _profile_config_dict["divisor"] = divisor_int
        #
        _profile_config_dict["include"] = profile_config_dict.get("include", [])
        
        return _profile_config_dict

    #

    def map1(self, map_function, profile_config_dict=None):
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
            topologyNode.pydbsp_step(liftedStream)
            #
            topologyNode.pydbsp_step(selection)
            #
            topologyNode.pydbsp_step(output_node)
        #
        def gc_function():
            topologyNode.pydbsp_gc(liftedStream)
            #
            topologyNode.pydbsp_gc(selection)
            #
            topologyNode.pydbsp_gc(output_node)
        #
        topologyNode = TopologyNode("map_op", output_handle_function, step_function, gc_function, [self], profile_config_dict)
        return topologyNode

    def map(self, map_function, profile_config_dict=None):
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
            topologyNode.pydbsp_step(liftedProject)
        #
        def gc_function():
            topologyNode.pydbsp_gc(liftedProject)
        #
        topologyNode = TopologyNode("map_op", output_handle_function, step_function, gc_function, [self], profile_config_dict)
        return topologyNode

    # def filter(self, filter_function, profile_config_dict=None):
    #     def filter_function1(value_json_str):
    #         value_dict = json.loads(value_json_str)
    #         return filter_function(value_dict)
    #     #
    #     liftedStream = LiftedStreamIntroduction(self._output_handle_function())
    #     #
    #     filtering = Filtering(liftedStream.output_handle(), filter_function1)
    #     #
    #     output_node = LiftedStreamElimination(filtering.output_handle())
    #     #
    #     def output_handle_function():
    #         return output_node.output_handle()
    #     #
    #     def step_function():
    #         topologyNode.pydbsp_step(liftedStream)
    #         #
    #         topologyNode.pydbsp_step(filtering)
    #         #
    #         topologyNode.pydbsp_step(output_node)
    #     #
    #     def gc_function():
    #         topologyNode.pydbsp_gc(liftedStream)
    #         #
    #         topologyNode.pydbsp_gc(filtering)
    #         #
    #         topologyNode.pydbsp_gc(output_node)
    #     #
    #     topologyNode = TopologyNode("filter_op", output_handle_function, step_function, gc_function, [self], profile_config_dict)
    #     return topologyNode

    def filter(self, filter_function, profile_config_dict=None):
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
            topologyNode.pydbsp_step(liftedSelect)
        #
        def gc_function():
            topologyNode.pydbsp_gc(liftedSelect)
        #
        topologyNode = TopologyNode("filter_op", output_handle_function, step_function, gc_function, [self], profile_config_dict)
        return topologyNode

    def limit(self, limit_int, profile_config_dict=None):
        def filter_function(_):
            self._counter_int += 1
            return self._counter_int <= limit_int
        #
        topologyNode = self.filter(filter_function, profile_config_dict=profile_config_dict)
        topologyNode._name_str = "limit_op"
        #
        return topologyNode

#     def left_join():
# (
#     SELECT a.id, a.name, b.value
#     FROM table_a a
#     INNER JOIN table_b b ON a.id = b.id
# )
# UNION
# (
#     SELECT a.id, a.name, NULL AS value
#     FROM table_a a
#     WHERE (a.id, a.name) NOT IN (
#         SELECT a.id, a.name
#         FROM table_a a
#         INNER JOIN table_b b ON a.id = b.id
#     )
# );

    def join(self, other, on_function, projection_function, profile_config_dict=None):
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
            topologyNode.pydbsp_gc(left_liftedStream)
            topologyNode.pydbsp_gc(right_liftedStream)
            #
            topologyNode.pydbsp_gc(join)
            #
            topologyNode.pydbsp_gc(output_node)
        #
        topologyNode = TopologyNode("join_op", output_handle_function, step_function, gc_function, [self, other], profile_config_dict)
        return topologyNode

    def join1(self, other, left_on_function, right_on_function, projection_function, profile_config_dict=None):
        def left_on_function1(left_value_json_str):
            left_value_dict = json.loads(left_value_json_str)
            return json.dumps(left_on_function(left_value_dict))
        #
        def right_on_function1(right_value_json_str):
            right_value_dict = json.loads(right_value_json_str)
            return json.dumps(right_on_function(right_value_dict))
        #
        def projection_function1(key, left_value_json_str, right_value_json_str):
            left_value_dict = json.loads(left_value_json_str)
            right_value_dict = json.loads(right_value_json_str)
            return json.dumps(projection_function(key, left_value_dict, right_value_dict))
        #
        left_liftedIndex = LiftedIndex(self._output_handle_function(), left_on_function1)
        right_liftedIndex = LiftedIndex(other._output_handle_function(), right_on_function1)
        incrementalize2 = Incrementalize2(
            left_liftedIndex.output_handle(),
            right_liftedIndex.output_handle(),
            lambda l, r: join_with_index(l, r, projection_function1),
            self._group)
        #
        def output_handle_function():
            return incrementalize2.output_handle()
        #
        def step_function():
            topologyNode.pydbsp_step(left_liftedIndex)
            topologyNode.pydbsp_step(right_liftedIndex)
            #
            topologyNode.pydbsp_step(incrementalize2)
        #
        def gc_function():
            topologyNode.pydbsp_gc(left_liftedIndex)
            topologyNode.pydbsp_gc(right_liftedIndex)
            #
            topologyNode.pydbsp_gc(incrementalize2)
        #
        topologyNode = TopologyNode("join1_op", output_handle_function, step_function, gc_function, [self, other], profile_config_dict)
        return topologyNode

    def join2(self, other, left_on_function, right_on_function, projection_function, profile_config_dict=None):
        def left_on_function1(left_value_json_str):
            left_value_dict = json.loads(left_value_json_str)
            return json.dumps(left_on_function(left_value_dict))
        #
        def right_on_function1(right_value_json_str):
            right_value_dict = json.loads(right_value_json_str)
            return json.dumps(right_on_function(right_value_dict))
        #
        def projection_function1(key, left_value_json_str, right_value_json_str):
            left_value_dict = json.loads(left_value_json_str)
            right_value_dict = json.loads(right_value_json_str)
            return json.dumps(projection_function(key, left_value_dict, right_value_dict))
        #
        left_liftedStreamIntroduction = LiftedStreamIntroduction(self._output_handle_function())
        right_liftedStreamIntroduction = LiftedStreamIntroduction(other._output_handle_function())
        #
        left_liftedLiftedIndex = LiftedLiftedIndex(left_liftedStreamIntroduction.output_handle(), left_on_function1)
        right_liftedLiftedIndex = LiftedLiftedIndex(right_liftedStreamIntroduction.output_handle(), right_on_function1)
        #
        deltaLiftedDeltaLiftedSortMergeJoin = DeltaLiftedDeltaLiftedSortMergeJoin(
            left_liftedLiftedIndex.output_handle(),
            right_liftedLiftedIndex.output_handle(),
            projection_function1)
        #
        output_node = LiftedStreamElimination(deltaLiftedDeltaLiftedSortMergeJoin.output_handle())
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            topologyNode.pydbsp_step(left_liftedStreamIntroduction)
            topologyNode.pydbsp_step(right_liftedStreamIntroduction)
            #
            topologyNode.pydbsp_step(left_liftedLiftedIndex)
            topologyNode.pydbsp_step(right_liftedLiftedIndex)
            #
            topologyNode.pydbsp_step(deltaLiftedDeltaLiftedSortMergeJoin)
            #
            topologyNode.pydbsp_step(output_node)
        #
        def gc_function():
            topologyNode.pydbsp_gc(left_liftedStreamIntroduction)
            topologyNode.pydbsp_gc(right_liftedStreamIntroduction)
            #
            topologyNode.pydbsp_gc(left_liftedLiftedIndex)
            topologyNode.pydbsp_gc(right_liftedLiftedIndex)
            #
            topologyNode.pydbsp_gc(deltaLiftedDeltaLiftedSortMergeJoin)
            #
            topologyNode.pydbsp_gc(output_node)
        #
        topologyNode = TopologyNode("join2_op", output_handle_function, step_function, gc_function, [self, other], profile_config_dict)
        return topologyNode

    def union(self, other, profile_config_dict=None):
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
        topologyNode = TopologyNode("union_op", output_handle_function, step_function, gc_function, [self, other], profile_config_dict)
        return topologyNode

    def intersect(self, other, profile_config_dict=None):
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
        topologyNode = TopologyNode("intersect_op", output_handle_function, step_function, gc_function, [self, other], profile_config_dict)
        return topologyNode

    def difference(self, other, profile_config_dict=None):
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
        topologyNode = TopologyNode("difference_op", output_handle_function, step_function, gc_function, [self, other], profile_config_dict)
        return topologyNode

    def product(self, other, profile_config_dict=None):
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
        topologyNode = TopologyNode("product_op", output_handle_function, step_function, gc_function, [self, other], profile_config_dict)
        return topologyNode
    
    def group_by_agg(self, by_function_list, as_function, agg_tuple_list, profile_config_dict=None):
        by_function = lambda value_dict: tuple(by_function(value_dict) for by_function in by_function_list)
        agg_select_function_agg_function_agg_as_function_tuple_list = agg_tuple_list
        agg_function = group_by_agg_fun([(agg_select_function, agg_function, agg_as_function, as_function) for agg_select_function, agg_function, agg_as_function in agg_select_function_agg_function_agg_as_function_tuple_list])
        #
        group_by_agg__topologyNode = self.group_by_agg_(by_function, agg_function, profile_config_dict=profile_config_dict)
        #
        return group_by_agg__topologyNode

    def group_by_agg_(self, by_function, agg_function, profile_config_dict=None):
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
            topologyNode.pydbsp_gc(lifted_stream_introduction)
            #
            topologyNode.pydbsp_gc(group_by_then_agg)
            #
            topologyNode.pydbsp_gc(lifted_stream_elimination)
            #
            topologyNode.pydbsp_gc(output_node)
        #
        topologyNode = TopologyNode("group_by_agg_op", output_handle_function, step_function, gc_function, [self], profile_config_dict)
        return topologyNode 

    def agg(self, agg_tuple_list, profile_config_dict=None):
        agg_select_function_agg_function_agg_as_function_tuple_list = agg_tuple_list
        agg_function = group_by_agg_fun([(agg_select_function, agg_function, agg_as_function, None) for agg_select_function, agg_function, agg_as_function in agg_select_function_agg_function_agg_as_function_tuple_list])
        #
        group_by_agg__topologyNode = self.group_by_agg_(lambda _: None, agg_function, profile_config_dict=profile_config_dict)
        group_by_agg__topologyNode._name_str = "agg_op"
        #
        return group_by_agg__topologyNode

    def peek(self, peek_function, profile_config_dict=None):
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
        topologyNode = TopologyNode("peek_op", output_handle_function, step_function, gc_function, [self], profile_config_dict)
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
        def traverse(topologyNode, stack_topologyNode_list):
            stack_topologyNode_list.append(topologyNode)
            for daughter_topologyNode in topologyNode.daughters():
                traverse(daughter_topologyNode, stack_topologyNode_list)
            return stack_topologyNode_list
        #
        stack_topologyNode_list = traverse(self, [])
        #
        while stack_topologyNode_list:
            topologyNode = stack_topologyNode_list.pop()
            #
            topologyNode.profile_before("step")
            #
            topologyNode._step_function()
            #
            topologyNode.profile_after("step")
    
    def gc(self):
        def traverse(topologyNode, stack_topologyNode_list):
            stack_topologyNode_list.append(topologyNode)
            for daughter_topologyNode in topologyNode.daughters():
                traverse(daughter_topologyNode, stack_topologyNode_list)
            return stack_topologyNode_list
        #
        stack_topologyNode_list = traverse(self, [])
        #
        while stack_topologyNode_list:
            topologyNode = stack_topologyNode_list.pop()
            #
            topologyNode.profile_before("gc")
            #
            topologyNode._gc_function()
            #
            topologyNode.profile_after("gc")
            #
            topologyNode.print_profile()

    # def gc(self):
    #     self.profile_before("gc")
    #     #
    #     self._gc_function()
    #     #
    #     self.profile_after("gc")
    #     #
    #     for topologyNode in self._daughter_topologyNode_list:
    #         topologyNode.gc()
    
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

    # profile_config_dict: {"step"/"gc": {"time": boolean, "memory": {"before": bool, "after": bool, "delta": bool}, "streams": list(str) ["dict", "len", "size"]}, "unit": str ("KB", "MB", "GB", "TB"), "divisor": int, "include": pydbsp_operator_class_name_str_list}
    
    def pydbsp_step(self, pydbsp_operator_object):
        if self._profile_config_dict is not None:
            self.pydbsp_profile_before(pydbsp_operator_object, "step")
        #
        pydbsp_operator_object.step()
        #
        if self._profile_config_dict is not None:
            self.pydbsp_profile_after(pydbsp_operator_object, "step")
       
    def pydbsp_gc(self, pydbsp_operator_object):
        if self._profile_config_dict is not None:
            self.pydbsp_profile_before(pydbsp_operator_object, "gc")
        #
        pydbsp_operator_object.gc()
        #
        if self._profile_config_dict is not None:
            self.pydbsp_profile_after(pydbsp_operator_object, "gc")

    #        

    def profile(self, step_or_gc_str, before_or_after_str):
        topologyNode_str = f"{self._name_str}_{self._id_str}"
        #
        if self._profile_config_dict is not None:
            _profile_config_dict1 = self._profile_config_dict[step_or_gc_str]
            #
            if _profile_config_dict1["time"]:
                self._profile_dict.setdefault(step_or_gc_str, {}).setdefault(topologyNode_str, {})[f"time_{before_or_after_str}"]  = time.time()
            #
            if _profile_config_dict1["memory"]["delta"] and before_or_after_str == "after":
                self._profile_dict.setdefault(step_or_gc_str, {}).setdefault(topologyNode_str, {})["memory_delta"] = len(pickle.dumps(self)) / self._profile_config_dict["divisor"] - self._profile_dict.get(step_or_gc_str, {}).get(topologyNode_str, {}).get("memory_after", 0)
            #
            if _profile_config_dict1["memory"][before_or_after_str]:
                self._profile_dict.setdefault(step_or_gc_str, {}).setdefault(topologyNode_str, {})[f"memory_{before_or_after_str}"] = len(pickle.dumps(self)) / self._profile_config_dict["divisor"]

    def profile_before(self, step_or_gc_str):
        self.profile(step_or_gc_str, "before")

    def profile_after(self, step_or_gc_str):
        self.profile(step_or_gc_str, "after")

    #
    
    def pydbsp_profile(self, pydbsp_operator, step_or_gc_str, before_or_after_str):
        topologyNode_str = f"{self._name_str}_{self._id_str}"
        pydbsp_operator_str = pydbsp_operator.__class__.__name__
        #
        if self._profile_config_dict is not None and pydbsp_operator_str in self._profile_config_dict["include"]:
            _profile_config_dict1 = self._profile_config_dict[step_or_gc_str]
            #
            if _profile_config_dict1["time"]:
                self._profile_dict.setdefault(step_or_gc_str, {}).setdefault(topologyNode_str, {}).setdefault(pydbsp_operator_str, {})[f"time_{before_or_after_str}"]  = time.time()
            #
            if _profile_config_dict1["memory"]["delta"] and before_or_after_str == "after" and pydbsp_operator_str in self._profile_config_dict["include"]:
                self._profile_dict.setdefault(step_or_gc_str, {}).setdefault(topologyNode_str, {}).setdefault(pydbsp_operator_str, {})["memory_delta"] = len(pickle.dumps(pydbsp_operator)) / self._profile_config_dict["divisor"] - self._profile_dict.get(step_or_gc_str, {}).get(topologyNode_str, {}).get(pydbsp_operator_str, {}).get("memory_after", 0)
            #
            if _profile_config_dict1["memory"][before_or_after_str] and pydbsp_operator_str in self._profile_config_dict["include"]:
                self._profile_dict.setdefault(step_or_gc_str, {}).setdefault(topologyNode_str, {}).setdefault(pydbsp_operator_str, {})[f"memory_{before_or_after_str}"] = len(pickle.dumps(pydbsp_operator)) / self._profile_config_dict["divisor"]
            #
            if _profile_config_dict1["streams"] is not None:
                pydbsp_profile_dict = pydbsp_operator.profile(_profile_config_dict1["streams"])
                self._profile_dict.setdefault(step_or_gc_str, {}).setdefault(topologyNode_str, {}).setdefault(pydbsp_operator_str, {})["streams"] = pydbsp_profile_dict

    def pydbsp_profile_before(self, pydbsp_operator, step_or_gc_str):
        self.pydbsp_profile(pydbsp_operator, step_or_gc_str, "before")

    def pydbsp_profile_after(self, pydbsp_operator, step_or_gc_str):
        self.pydbsp_profile(pydbsp_operator, step_or_gc_str, "after")

    def print_profile(self):
        if self._profile_config_dict is not None:
            print(json.dumps(self._profile_dict, indent=2))
            # print(self._profile_dict)

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

def source(name_str, profile_config_dict=None):
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
        stream_handle.get().gc()
    #
    topologyNode = TopologyNode(name_str, output_handle_function, step_function, gc_function, [], profile_config_dict)
    return topologyNode


def json_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    # if isinstance(obj, Decimal):
    #     return float(obj)
    # if isinstance(obj, UUID):
    #     return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def message_dict_list_to_ZSet(message_dict_list):
    value_json_str_list = []
    for message_dict in message_dict_list:
        value_dict = message_dict["value"]
        # value_dict["_key"] = message_dict["key"]
        value_json_str = json.dumps(value_dict, default=json_default)
        value_json_str_list.append(value_json_str)
    #
    zSet = ZSet({k: 1 for k in value_json_str_list})
    return zSet
