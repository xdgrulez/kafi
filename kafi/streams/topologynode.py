from pydbsp.indexed_zset.functions.bilinear import join_with_index
from pydbsp.indexed_zset.operators.linear import LiftedIndex, LiftedLiftedIndex
from pydbsp.indexed_zset.operators.bilinear import DeltaLiftedDeltaLiftedSortMergeJoin
from pydbsp.stream import step_until_fixpoint_and_return, StreamHandle
from pydbsp.stream.functions.linear import stream_elimination
from pydbsp.zset.operators.linear import LiftedSelect, LiftedProject
from pydbsp.stream.operators.linear import LiftedStreamIntroduction, LiftedStreamElimination
from pydbsp.stream.operators.bilinear import Incrementalize2

import json

#

class TopologyNode:
    def __init__(self, name_str="", output_handle_function=lambda: None, step_function=lambda: None, daughter_topologyNode_list=[]):
        self._name_str = name_str
        self._output_handle_function = output_handle_function
        self._step_function = step_function
        self._daughter_topologyNode_list = daughter_topologyNode_list
        #
        self.group = output_handle_function().get().group()

    def map(self, map_function):
        def map_function1(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(map_function(message_dict))
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
        def filter_function1(message_json_str):
            message_dict = json.loads(message_json_str)
            return filter_function(message_dict)
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
        def on_function1(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(on_function(message_dict))
        #
        def projection_function1(key, left_message_json_str, right_message_json_str):
            left_message_dict = json.loads(left_message_json_str)
            right_message_dict = json.loads(right_message_json_str)
            return json.dumps(projection_function(key, left_message_dict, right_message_dict))
        #
        index_function = lambda x: on_function1(x)
        left_index = LiftedIndex(self._output_handle_function(), index_function)
        right_index = LiftedIndex(other._output_handle_function(), index_function)
        join_op = Incrementalize2(
            left_index.output_handle(),
            right_index.output_handle(),
            lambda l, r: join_with_index(l, r, projection_function1),
            self.group
        )
        def output_handle_function():
            return join_op.output_handle()
        #
        def step_function():
            left_index.step()
            right_index.step()
            join_op.step()
        #
        return TopologyNode("join_op", output_handle_function, step_function, [self, other])

    def join2(self, other, on_function, projection_function):
        def on_function1(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(on_function(message_dict))
        #
        def projection_function1(key, left_message_json_str, right_message_json_str):
            left_message_dict = json.loads(left_message_json_str)
            right_message_dict = json.loads(right_message_json_str)
            return json.dumps(projection_function(key, left_message_dict, right_message_dict))
        #
        l1 = LiftedStreamIntroduction(self._output_handle_function())
        r1 = LiftedStreamIntroduction(other._output_handle_function())
        #
        index_function = lambda x: on_function1(x)
        l2 = LiftedLiftedIndex(l1.output_handle(), index_function)
        r2 = LiftedLiftedIndex(r1.output_handle(), index_function)
        #
        deltaLiftedDeltaLiftedSortMergeJoin = DeltaLiftedDeltaLiftedSortMergeJoin(
            l2.output_handle(),
            r2.output_handle(),
            projection_function1)
        #
        output_node = LiftedStreamElimination(
            deltaLiftedDeltaLiftedSortMergeJoin.output_handle()
        )
        #
        def output_handle_function():
            return output_node.output_handle()
        #
        def step_function():
            l1.step()
            r1.step()
            l2.step()
            r2.step()
            deltaLiftedDeltaLiftedSortMergeJoin.step()
            step_until_fixpoint_and_return(output_node)
        #
        return TopologyNode("join2_op", output_handle_function, step_function, [self, other])

    def peek(self, peek_function):
        def peek_function1(message_json_str):
            message_dict = json.loads(message_json_str)
            peek_function(message_dict)
            return message_json_str
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
    
    #

    def name(self):
        return self._name_str

    def output_handle_function(self):
        return self._output_handle_function

    def step_function(self):
        return self._step_function

    def daughters(self):
        return self._daughter_topologyNode_list    

    #

    def latest(self):
        return self._output_handle_function().get().latest()

    #

    def topology(self):
        daughters_int = len(self._daughter_topologyNode_list)
        match daughters_int:
            case 0:
                return self._name_str
            case 1:
                return f"{self._name_str}({self._daughter_topologyNode_list[0].topology()})"
            case 2:
                return  f"{self._name_str}({self._daughter_topologyNode_list[0].topology()}, {self._daughter_topologyNode_list[1].topology()})"

    def mermaid(self):
        def collect_nodes(topologyNode, name_str_set, edge_str_list):
            name_str_set.add(topologyNode.name())
            #
            for daughter_topologyNode in topologyNode.daughters():
                edge_str_list.append(f"{daughter_topologyNode.name()} --> {topologyNode.name()}")
                collect_nodes(daughter_topologyNode, name_str_set, edge_str_list)
        #
        node_str_set = set()
        edge_str_list = []
        collect_nodes(self, node_str_set, edge_str_list)
        #
        mermaid_str = ["graph TD"]
        mermaid_str.extend(f"    {name_str}" for name_str in node_str_set)
        mermaid_str.extend(f"    {edge_str}" for edge_str in edge_str_list)
        #
        return "\n".join(mermaid_str)
