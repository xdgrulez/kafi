from pydbsp.indexed_zset.functions.bilinear import join_with_index
from pydbsp.indexed_zset.operators.linear import LiftedIndex
from pydbsp.stream.operators.bilinear import Incrementalize2
from pydbsp.zset.operators.linear import LiftedSelect, LiftedProject
import json

#

class TopologyNode:
    def __init__(self, stream_handle, name="", operators=[], parents=[]):
        self._stream_handle = stream_handle
        self._name = name
        self._operators = operators
        self._parents = parents
        #
        self.group = stream_handle.get().group()

    def map(self, map_function):
        def projection_(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(map_function(message_dict))
        #
        project_op = LiftedProject(self._stream_handle, projection_)
        return TopologyNode(project_op.output_handle(), "map_op", [project_op], [self])

    def filter(self, filter_function):
        def predicate_(message_json_str):
            message_dict = json.loads(message_json_str)
            return filter_function(message_dict)
        #
        liftedSelect = LiftedSelect(self._stream_handle, predicate_)
        return TopologyNode(liftedSelect.output_handle(), "filter_op", [liftedSelect], [self])

    def join(self, other, on_function, projection_function):
        def on_(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(on_function(message_dict))
        #
        def projection_(key, left_message_json_str, right_message_json_str):
            left_message_dict = json.loads(left_message_json_str)
            right_message_dict = json.loads(right_message_json_str)
            return json.dumps(projection_function(key, left_message_dict, right_message_dict))
        #
        indexer = lambda x: on_(x)
        left_index = LiftedIndex(self._stream_handle, indexer)
        right_index = LiftedIndex(other._stream_handle, indexer)
        join_op = Incrementalize2(
            left_index.output_handle(),
            right_index.output_handle(),
            lambda l, r: join_with_index(l, r, projection_),
            self.group
        )
        return TopologyNode(join_op.output_handle(), "join_op", [left_index, right_index, join_op], [self, other])

    def peek(self, peek_function):
        def projection_(message_json_str):
            message_dict = json.loads(message_json_str)
            peek_function(message_dict)
            return message_json_str
        #
        project_op = LiftedProject(self._stream_handle, projection_)
        return TopologyNode(project_op.output_handle(), "peek_op", [project_op], [self])
    
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
            for op in topologyNode._operators:
                op.step()
    
    #

    def topology(self):
        parents_int = len(self._parents)
        match parents_int:
            case 0:
                return self._name
            case 1:
                return f"{self._name}({self._parents[0].topology()})"
            case 2:
                return  f"{self._name}({self._parents[0].topology()}, {self._parents[1].topology()})"

    def parents(self):
        return self._parents

    def name(self):
        return self._name

    def stream_handle(self):
        return self._stream_handle

    def stream(self):
        return self._stream_handle.get()
        
    def latest(self):
        return self._operators[-1].output().latest()
