from pydbsp.indexed_zset.functions.bilinear import join_with_index
from pydbsp.indexed_zset.operators.linear import LiftedIndex
from pydbsp.stream.operators.bilinear import Incrementalize2
from pydbsp.zset.operators.linear import LiftedSelect, LiftedProject
import json

#

class TopologyNode:
    def __init__(self, stream_handle, name_str="", operator_list=[], parent_topologyNode_list=[]):
        self._stream_handle = stream_handle
        self._name_str = name_str
        self._operator_list = operator_list
        self._parent_topologyNode_list = parent_topologyNode_list
        #
        self.group = stream_handle.get().group()

    def map(self, map_fun):
        def map_fun1(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(map_fun(message_dict))
        #
        liftedProject = LiftedProject(self._stream_handle, map_fun1)
        return TopologyNode(liftedProject.output_handle(), "map_op", [liftedProject], [self])

    def filter(self, filter_fun):
        def filter_fun1(message_json_str):
            message_dict = json.loads(message_json_str)
            return filter_fun(message_dict)
        #
        liftedSelect = LiftedSelect(self._stream_handle, filter_fun1)
        return TopologyNode(liftedSelect.output_handle(), "filter_op", [liftedSelect], [self])

    def join(self, other, on_fun, projection_fun):
        def on_fun1(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(on_fun(message_dict))
        #
        def projection_fun1(key, left_message_json_str, right_message_json_str):
            left_message_dict = json.loads(left_message_json_str)
            right_message_dict = json.loads(right_message_json_str)
            return json.dumps(projection_fun(key, left_message_dict, right_message_dict))
        #
        index_fun = lambda x: on_fun1(x)
        left_liftedIndex = LiftedIndex(self._stream_handle, index_fun)
        right_liftedIndex = LiftedIndex(other._stream_handle, index_fun)
        incrementalize2 = Incrementalize2(
            left_liftedIndex.output_handle(),
            right_liftedIndex.output_handle(),
            lambda left_indexed_ZSet, right_indexed_ZSet: join_with_index(left_indexed_ZSet, right_indexed_ZSet, projection_fun1),
            self.group
        )
        return TopologyNode(incrementalize2.output_handle(), "join_op", [left_liftedIndex, right_liftedIndex, incrementalize2], [self, other])

    def peek(self, peek_fun):
        def peek_fun1(message_json_str):
            message_dict = json.loads(message_json_str)
            peek_fun(message_dict)
            return message_json_str
        #
        liftedProject = LiftedProject(self._stream_handle, peek_fun1)
        return TopologyNode(liftedProject.output_handle(), "peek_op", [liftedProject], [self])
    
    #

    def step(self):
        def traverse(topologyNode, stack):
            stack.append(topologyNode)
            for parent in topologyNode.parents():
                traverse(parent, stack)
            return stack
        #
        stack = traverse(self, [])
        #
        while stack:
            topologyNode = stack.pop()
            for operator in topologyNode.operators():
                operator.step()
    
    #

    def stream_handle(self):
        return self._stream_handle

    def stream(self):
        return self._stream_handle.get()

    def name(self):
        return self._name_str

    def operators(self):
        return self._operator_list

    def parents(self):
        return self._parent_topologyNode_list
        
    #

    def latest(self):
        return self._operator_list[-1].output().latest()

    #

    def topology(self):
        parents_int = len(self._parent_topologyNode_list)
        match parents_int:
            case 0:
                return self._name_str
            case 1:
                return f"{self._name_str}({self._parent_topologyNode_list[0].topology()})"
            case 2:
                return  f"{self._name_str}({self._parent_topologyNode_list[0].topology()}, {self._parent_topologyNode_list[1].topology()})"

    def mermaid(self):
        def collect_nodes(topologyNode, name_str_set, edge_str_list):
            name_str_set.add(topologyNode.name())
            #
            for parent_topologyNode in topologyNode.parents():
                edge_str_list.append(f"{parent_topologyNode.name()} --> {topologyNode.name()}")
                collect_nodes(parent_topologyNode, name_str_set, edge_str_list)
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
