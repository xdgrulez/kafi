from pydbsp.indexed_zset.functions.bilinear import join_with_index
from pydbsp.indexed_zset.operators.linear import LiftedIndex
from pydbsp.stream import Stream, StreamHandle
from pydbsp.stream.operators.bilinear import Incrementalize2
from pydbsp.zset import ZSet, ZSetAddition
from pydbsp.zset.operators.linear import LiftedSelect, LiftedProject
import json

class KS:
    def __init__(self, stream_handle, name="", operators=[], parents=[]):
        self._stream_handle = stream_handle
        self._name = name
        self._operators = operators
        self._parents = parents
        #
        self._group = stream_handle.get().group()

    def select(self, projection):
        def projection_(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(projection(message_dict))
        #
        project_op = LiftedProject(self._stream_handle, projection_)
        return KS(project_op.output_handle(), "select_op", [project_op], [self])

    def where(self, predicate):
        def predicate_(message_json_str):
            message_dict = json.loads(message_json_str)
            return predicate(message_dict)
        #
        liftedSelect = LiftedSelect(self._stream_handle, predicate_)
        return KS(liftedSelect.output_handle(), "where_op", [liftedSelect], [self])

    def join(self, other, on, projection):
        def on_(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(on(message_dict))
        #
        def projection_(key, left_message_json_str, right_message_json_str):
            left_message_dict = json.loads(left_message_json_str)
            right_message_dict = json.loads(right_message_json_str)
            return json.dumps(projection(key, left_message_dict, right_message_dict))
        #
        indexer = lambda x: on_(x)
        left_index = LiftedIndex(self._stream_handle, indexer)
        right_index = LiftedIndex(other._stream_handle, indexer)
        join_op = Incrementalize2(
            left_index.output_handle(),
            right_index.output_handle(),
            lambda l, r: join_with_index(l, r, projection_),
            self._group
        )
        return KS(join_op.output_handle(), "join_op", [left_index, right_index, join_op], [self, other])

    def step(self):
        def traverse(node, stack):
            stack.append(node)
            for parent in node.parents():
                traverse(parent, stack)
            return stack
        #
        stack = traverse(self, [])
        #
        while stack:
            node = stack.pop()
            for op in node._operators:
                # print(op)
                op.step()

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

    def stream(self):
        return self._stream_handle.get()
    
    def latest(self):
        return self._operators[-1].output().latest()

#

def demo():
    employee_message_dict_list = [{"key": "0", "value": {"name": "kristjan"}},
                                {"key": "1", "value": {"name": "mark"}},
                                {"key": "2", "value": {"name": "mike"}}]
    salary_message_dict_list = [{"key": "2", "value": {"salary": 40000}},
                                {"key": "0", "value": {"salary": 38750}},
                                {"key": "1", "value": {"salary": 50000}}]

    def message_dict_list_to_zset(message_dict_list):
        message_str_list = [json.dumps(message_dict) for message_dict in message_dict_list]
        zSet = ZSet({k: 1 for k in message_str_list})
        return zSet

    def create_stream():
        stream = Stream(ZSetAddition())
        stream_handle = StreamHandle(lambda: stream)
        return stream_handle

    employees_stream_handle = create_stream()
    salaries_stream_handle = create_stream()

    employee_zset = message_dict_list_to_zset(employee_message_dict_list)
    salary_zset = message_dict_list_to_zset(salary_message_dict_list)

    employees_stream_handle.get().send(employee_zset)
    salaries_stream_handle.get().send(salary_zset)

    employees_node = KS(employees_stream_handle, "employees_source")
    salaries_node = KS(salaries_stream_handle, "salaries_source")

    def sel(message_dict):
        message_dict["value"]["name"] = message_dict["value"]["name"] + "_abc"
        return message_dict

    def proj(_, left_message_dict, right_message_dict):
        left_message_dict["value"].update(right_message_dict["value"])
        return left_message_dict

    topology = (
        employees_node
        .where(lambda message_dict: message_dict["value"]["name"] != "mark")
        .join(
            salaries_node, 
            on=lambda message_dict: message_dict["key"],
            projection=proj
        )
        .select(sel)
    )

    topology.step()

    print()
    print(f"Topology: {topology.topology()}")
    print()
    print(f"Latest: {topology.latest()}")

if __name__ == "__main__":
    demo()
 