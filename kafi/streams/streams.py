from pydbsp.stream import Stream, StreamHandle
from pydbsp.zset import ZSet, ZSetAddition

from asyncio import TaskGroup, Queue, sleep
import json

from kafi.streams.topologynode import TopologyNode

#

def source(name):
    stream = Stream(ZSetAddition())
    stream_handle = StreamHandle(lambda: stream)
    return TopologyNode(stream_handle, name)

#

def message_dict_list_to_zset(message_dict_list):
    message_str_list = [json.dumps(message_dict) for message_dict in message_dict_list]
    zSet = ZSet({k: 1 for k in message_str_list})
    return zSet

#

async def streams(storage_source_topologyNode_tuple_list, root_topologyNode, sink_storage, sink_topic_str, **kwargs):
    producer = sink_storage.producer(sink_topic_str, **kwargs)
    #
    def sink_fun(message_dict_list):
        producer.produce_list(message_dict_list, **kwargs)
    #
    def finally_fun():
        producer.close()
    #
    await streams_fun(storage_source_topologyNode_tuple_list, root_topologyNode, sink_fun, finally_fun, **kwargs)

async def streams_fun(storage_source_topologyNode_tuple_list, root_topologyNode, foreach_function, finally_function, **kwargs):
    consume_sleep_int = kwargs["consume_sleep"] if "consume_sleep" in kwargs else 0.2
    process_sleep_int = kwargs["process_sleep"] if "process_sleep" in kwargs else 0.2
    #
    storage_source_topologyNode_queue_tuple_list = []
    for storage, source_topologyNode in storage_source_topologyNode_tuple_list:
        queue = Queue()
        storage_source_topologyNode_queue_tuple_list.append((storage, source_topologyNode, queue))
    #
    async def consumer_task(storage, topic_str, queue):
        consumer = storage.consumer(topic_str, **kwargs)
        try:
            while True:
                message_dict_list = consumer.consume(**kwargs)
                if message_dict_list != []:
                    await queue.put(message_dict_list)
                await sleep(consume_sleep_int)
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
    #
    async def process():
        try:
            while True:
                for _, source_topologyNode, queue in storage_source_topologyNode_queue_tuple_list:
                    if not queue.empty():
                        message_dict_list = await queue.get()
                        zset = message_dict_list_to_zset(message_dict_list)
                        #
                        stream = source_topologyNode.stream()
                        stream.send(zset)
                #
                root_topologyNode.step()
                #
                zset = root_topologyNode.latest()
                message_dict_list = [json.loads(message_json_str) for message_json_str, i in zset.items() if i == 1]
                #
                foreach_function(message_dict_list)
                #
                await sleep(process_sleep_int)
        except KeyboardInterrupt:
            pass
        finally:
            finally_function()
    #
    async with TaskGroup() as taskGroup:
        for storage, source_topologyNode, queue in storage_source_topologyNode_queue_tuple_list:
            topic_str = source_topologyNode.name()
            taskGroup.create_task(consumer_task(storage, topic_str, queue))
        #
        taskGroup.create_task(process())
