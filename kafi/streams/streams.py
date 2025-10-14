from kafi.streams.topologynode import message_dict_list_to_ZSet
from asyncio import TaskGroup, Queue, sleep
import json

#

async def streams(storage_source_topologyNode_tuple_list, root_topologyNode, sink_storage, sink_topic_str, **kwargs):
    producer = sink_storage.producer(sink_topic_str, **kwargs)
    #
    def sink_function(message_dict_list):
        producer.produce_list(message_dict_list, **kwargs)
    #
    def finally_function():
        producer.close()
    #
    await streams_function(storage_source_topologyNode_tuple_list, root_topologyNode, sink_function, finally_function, **kwargs)

async def streams_function(storage_source_topologyNode_tuple_list, root_topologyNode, foreach_function, finally_function, **kwargs):
    consume_sleep_int = kwargs["consume_sleep"] if "consume_sleep" in kwargs else 0.2
    process_sleep_int = kwargs["process_sleep"] if "process_sleep" in kwargs else 0.2
    #
    storage_source_topologyNode_queue_tuple_list = []
    for storage, source_topologyNode in storage_source_topologyNode_tuple_list:
        queue = Queue()
        storage_source_topologyNode_queue_tuple_list.append((storage, source_topologyNode, queue))
    #
    storage_id_topic_str_tuple_partitions_int_dict = {}
    for storage, source_topologyNode in storage_source_topologyNode_tuple_list:
        topic_str = source_topologyNode.name()
        partitions_int = storage.partitions(topic_str)[topic_str]
        #
        storage_id = storage.get_id()
        storage_id_topic_str_tuple_partitions_int_dict[(storage_id, topic_str)] = partitions_int
    #
    storage_id_topic_str_tuple_consumer_dict = {}
    for storage, source_topologyNode in storage_source_topologyNode_tuple_list:
        topic_str = source_topologyNode.name()
        consumer = storage.consumer(topic_str, **kwargs)
        #
        storage_id = storage.get_id()
        storage_id_topic_str_tuple_consumer_dict[(storage_id, topic_str)] = consumer
    #
    async def consumer_task(consumer, queue):
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
    def get_latest_offset(message_dict_list, partition_int):
        return next((message_dict["offset"] for message_dict in reversed(message_dict_list) if message_dict["partition"] == partition_int), None)
    #
    async def process():
        try:
            while True:
                storage_id_topic_str_tuple_offsets_dict_dict = {}
                for storage, source_topologyNode, queue in storage_source_topologyNode_queue_tuple_list:
                    if not queue.empty():
                        message_dict_list = await queue.get()
                        #
                        topic_str = source_topologyNode.name()
                        storage_id_topic_str_tuple = (storage.get_id(), topic_str)
                        partitions_int = storage_id_topic_str_tuple_partitions_int_dict[storage_id_topic_str_tuple]
                        storage_id_topic_str_tuple_offsets_dict_dict[storage_id_topic_str_tuple] = {}
                        for partition_int in range(partitions_int):
                            offset_int = get_latest_offset(message_dict_list, partition_int)
                            # By convention, committed offsets reflect the next message to be consumed, not the last message consumed. (from https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
                            if offset_int is not None:
                                storage_id_topic_str_tuple_offsets_dict_dict[storage_id_topic_str_tuple][partition_int] = offset_int + 1
                        #
                        zSet = message_dict_list_to_ZSet(message_dict_list)
                        #
                        stream = source_topologyNode.output_handle_function()().get()
                        stream.send(zSet)
                #
                root_topologyNode.step()
                #
                zSet = root_topologyNode.latest()
                message_dict_list = [json.loads(message_json_str) for message_json_str, i in zSet.items() if i == 1]
                #
                foreach_function(message_dict_list)
                #
                for storage_id_topic_str_tuple, offsets_dict in storage_id_topic_str_tuple_offsets_dict_dict.items():
                    consumer = storage_id_topic_str_tuple_consumer_dict[storage_id_topic_str_tuple]
                    print(f"Committed {offsets_dict} for topic {storage_id_topic_str_tuple[1]}")
                    consumer.commit(offsets_dict)
                #
                await sleep(process_sleep_int)
        except KeyboardInterrupt:
            pass
        finally:
            finally_function()
    #
    async with TaskGroup() as taskGroup:
        for storage, source_topologyNode, queue in storage_source_topologyNode_queue_tuple_list:
            storage_id = storage.get_id()
            topic_str = source_topologyNode.name()
            consumer = storage_id_topic_str_tuple_consumer_dict[(storage_id, topic_str)]
            taskGroup.create_task(consumer_task(consumer, queue))
        #
        taskGroup.create_task(process())
