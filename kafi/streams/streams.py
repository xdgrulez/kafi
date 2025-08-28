from pydbsp.indexed_zset.functions.bilinear import join_with_index
from pydbsp.indexed_zset.operators.linear import LiftedIndex
from pydbsp.stream import Stream, StreamHandle
from pydbsp.stream.operators.bilinear import Incrementalize2
from pydbsp.zset import ZSet, ZSetAddition
from pydbsp.zset.operators.linear import LiftedSelect, LiftedProject

from asyncio import TaskGroup, Queue, sleep
import json

class Node:
    def __init__(self, stream_handle, name="", operators=[], parents=[]):
        self._stream_handle = stream_handle
        self._name = name
        self._operators = operators
        self._parents = parents
        #
        self.group = stream_handle.get().group()

    def select(self, projection):
        def projection_(message_json_str):
            message_dict = json.loads(message_json_str)
            return json.dumps(projection(message_dict))
        #
        project_op = LiftedProject(self._stream_handle, projection_)
        return Node(project_op.output_handle(), "select_op", [project_op], [self])

    def where(self, predicate):
        def predicate_(message_json_str):
            message_dict = json.loads(message_json_str)
            return predicate(message_dict)
        #
        liftedSelect = LiftedSelect(self._stream_handle, predicate_)
        return Node(liftedSelect.output_handle(), "where_op", [liftedSelect], [self])

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
            self.group
        )
        return Node(join_op.output_handle(), "join_op", [left_index, right_index, join_op], [self, other])

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

    def name(self):
        return self._name

    def stream_handle(self):
        return self._stream_handle

    def stream(self):
        return self._stream_handle.get()
        
    def latest(self):
        return self._operators[-1].output().latest()

#

def source(name):
    stream = Stream(ZSetAddition())
    stream_handle = StreamHandle(lambda: stream)
    return Node(stream_handle, name)

#

def message_dict_list_to_zset(message_dict_list):
    message_str_list = [json.dumps(message_dict) for message_dict in message_dict_list]
    zSet = ZSet({k: 1 for k in message_str_list})
    return zSet

#

async def run(storage_source_node_tuple_list, sink_storage, sink_topic_str, root_node, **kwargs):
    storage_source_node_queue_tuple_list = []
    for storage, source_node in storage_source_node_tuple_list:
        queue = Queue()
        storage_source_node_queue_tuple_list.append((storage, source_node, queue))
    #
    async def consumer_task(storage, topic_str, queue):
        consumer = storage.consumer(topic_str, **kwargs)
        try:
            while True:
                message_dict_list = consumer.consume(**kwargs)
                if message_dict_list != []:
                    await queue.put(message_dict_list)
                await sleep(0.1)
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
    #
    async def process():
        producer = sink_storage.producer(sink_topic_str, **kwargs)
        try:
            while True:
                for _, source_node, queue in storage_source_node_queue_tuple_list:
                    message_dict_list = await queue.get()
                    zset = message_dict_list_to_zset(message_dict_list)
                    #
                    stream = source_node.stream()
                    stream.send(zset)
                #
                root_node.step()
                #
                zset = root_node.latest()
                message_dict_list = [json.loads(message_json_str) for message_json_str, i in zset.items() if i == 1]
                producer.produce_list(message_dict_list, **kwargs)
                #
                await sleep(0.1)
        except KeyboardInterrupt:
            pass
        finally:
            producer.close()
    #
    async with TaskGroup() as taskGroup:
        for storage, source_node, queue in storage_source_node_queue_tuple_list:
            topic_str = source_node.name()
            taskGroup.create_task(consumer_task(storage, topic_str, queue))
        #
        taskGroup.create_task(process())

#

def demo():
    employee_message_dict_list = [{"key": "0", "value": {"name": "kristjan"}},
                                {"key": "1", "value": {"name": "mark"}},
                                {"key": "2", "value": {"name": "mike"}}]
    salary_message_dict_list = [{"key": "2", "value": {"salary": 40000}},
                                {"key": "0", "value": {"salary": 38750}},
                                {"key": "1", "value": {"salary": 50000}}]

    def sel(message_dict):
        message_dict["value"]["name"] = message_dict["value"]["name"] + "_abc"
        return message_dict

    def proj(_, left_message_dict, right_message_dict):
        left_message_dict["value"].update(right_message_dict["value"])
        return left_message_dict

    employees_source = source("employees_source")
    salaries_source = source("salaries_source")

    topology = (
        employees_source
        .where(lambda message_dict: message_dict["value"]["name"] != "mark")
        .join(
            salaries_source,
            on=lambda message_dict: message_dict["key"],
            projection=proj
        )
        .select(sel)
    )

    employee_zset = message_dict_list_to_zset(employee_message_dict_list)
    salary_zset = message_dict_list_to_zset(salary_message_dict_list)
    employees_source.stream_handle().get().send(employee_zset)
    salaries_source.stream_handle().get().send(salary_zset)

    topology.step()

    print()
    print(f"Topology: {topology.topology()}")
    print()
    print(f"Latest: {topology.latest()}")

if __name__ == "__main__":
    demo()
 