from pydbsp.indexed_zset.functions.bilinear import join_with_index
from pydbsp.indexed_zset.operators.linear import LiftedIndex
from pydbsp.stream.operators.bilinear import Incrementalize2
from pydbsp.zset.operators.linear import LiftedSelect, LiftedProject
import json

#

class TopologyNode:
    def __init__(self, stream_handle, name_str="", operator_list=[], daughter_topologyNode_list=[], state_function_tuple=None):
        self._stream_handle = stream_handle
        self._name_str = name_str
        self._operator_list = operator_list
        self._daughter_topologyNode_list = daughter_topologyNode_list
        #
        self._state_function_tuple = state_function_tuple
        #
        self.group = stream_handle.get().group()

    def map(self, map_function):
        def map_function1(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(map_function(message_dict))
        #
        liftedProject = LiftedProject(self._stream_handle, map_function1)
        return TopologyNode(liftedProject.output_handle(), "map_op", [liftedProject], [self])

    def filter(self, filter_function):
        def filter_function1(message_json_str):
            message_dict = json.loads(message_json_str)
            return filter_function(message_dict)
        #
        liftedSelect = LiftedSelect(self._stream_handle, filter_function1)
        return TopologyNode(liftedSelect.output_handle(), "filter_op", [liftedSelect], [self])

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
        left_liftedIndex = LiftedIndex(self._stream_handle, index_function)
        right_liftedIndex = LiftedIndex(other._stream_handle, index_function)
        incrementalize2 = Incrementalize2(
            left_liftedIndex.output_handle(),
            right_liftedIndex.output_handle(),
            lambda left_indexed_ZSet, right_indexed_ZSet: join_with_index(left_indexed_ZSet, right_indexed_ZSet, projection_function1),
            self.group
        )
        #
        def get_state():
            return (incrementalize2.integrated_stream_a,
                    incrementalize2.delayed_integrated_stream_a,
                    incrementalize2.integrated_stream_b,
                    incrementalize2.delayed_integrated_stream_b)
        #
        def set_state(state_tuple):
            incrementalize2.integrated_stream_a = state_tuple[0]
            incrementalize2.delayed_integrated_stream_a = state_tuple[1]
            incrementalize2.integrated_stream_b = state_tuple[2]
            incrementalize2.delayed_integrated_stream_b = state_tuple[3]
        #
        return TopologyNode(incrementalize2.output_handle(), "join_op", [left_liftedIndex, right_liftedIndex, incrementalize2], [self, other], (get_state, set_state))

    def peek(self, peek_function):
        def peek_function1(message_json_str):
            message_dict = json.loads(message_json_str)
            peek_function(message_dict)
            return message_json_str
        #
        liftedProject = LiftedProject(self._stream_handle, peek_function1)
        return TopologyNode(liftedProject.output_handle(), "peek_op", [liftedProject], [self])
    
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

    def daughters(self):
        return self._daughter_topologyNode_list
        
    #

    def latest(self):
        return self._operator_list[-1].output().latest()

    #

    def get_state(self):
        return {"state": self._state_function_tuple[0]() if self._state_function_tuple is not None else None,
                "daughters": [daughter_topologyNode.get_state() for daughter_topologyNode in self._daughter_topologyNode_list]}

    def set_state(self, state_dict):
        if state_dict["state"] is not None:
            self._state_function_tuple[1](state_dict["state"])
        #
        for i in range(len(self._daughter_topologyNode_list)):
            self._daughter_topologyNode_list[i].set_state(state_dict["daughters"][i])

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
