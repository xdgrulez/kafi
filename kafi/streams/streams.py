from kafi.streams.topologynode import message_dict_list_to_ZSet
from kafi.helpers import get_millis
from asyncio import TaskGroup, Queue, sleep
import json
import cloudpickle as pickle

#

async def streams(storage_topic_str_tuple_list, root_topologyNode, sink_storage, sink_topic_str, snapshot_storage=None, snapshot_topic=None, stop_thread=None, **kwargs):
    producer = sink_storage.producer(sink_topic_str, **kwargs)
    #
    def sink_function(message_dict_list):
        producer.produce_list(message_dict_list, **kwargs)
    #
    def finally_function():
        producer.close()
    #
    await streams_function(storage_topic_str_tuple_list, root_topologyNode, sink_function, finally_function, snapshot_storage, snapshot_topic, stop_thread, **kwargs)

async def streams_function(storage_topic_str_tuple_list, root_topologyNode, foreach_function, finally_function, snapshot_storage=None, snapshot_topic=None, stop_thread=None, **kwargs):
    consume_sleep_float = kwargs["consume_sleep"] if "consume_sleep" in kwargs else 0.2
    process_sleep_float = kwargs["process_sleep"] if "process_sleep" in kwargs else 0.2
    snapshot_interval_float = kwargs["snapshot_interval"] if "snapshot_interval" in kwargs else 1.0
    #
    initial_time_int = get_millis()
    #
    storage_source_topologyNode_queue_tuple_list = []
    for storage, topic_str in storage_topic_str_tuple_list:
        source_topologyNode = root_topologyNode.get_node_by_name(topic_str)
        #
        queue = Queue()
        storage_source_topologyNode_queue_tuple_list.append((storage, source_topologyNode, queue))
    #
    storage_id_topic_str_tuple_partitions_int_dict = {}
    for storage, topic_str in storage_topic_str_tuple_list:
        partitions_int = storage.partitions(topic_str)[topic_str]
        #
        storage_id = storage.get_id()
        storage_id_topic_str_tuple_partitions_int_dict[(storage_id, topic_str)] = partitions_int
    #
    storage_id_topic_str_tuple_consumer_dict = {}
    for storage, topic_str in storage_topic_str_tuple_list:
        consumer = storage.consumer(topic_str, **kwargs)
        #
        storage_id = storage.get_id()
        storage_id_topic_str_tuple_consumer_dict[(storage_id, topic_str)] = consumer
    #
    async def consumer_task(consumer, queue):
        try:
            while True and (stop_thread is None or not stop_thread.is_set()):
                message_dict_list = consumer.consume(**kwargs)
                if message_dict_list != []:
                    print("Read...")
                    print(message_dict_list)
                    print("---")
                    await queue.put(message_dict_list)
                await sleep(consume_sleep_float)
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
    #
    async def process():
        try:
            while True and (stop_thread is None or not stop_thread.is_set()):
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
                zSet = root_topologyNode.latest_until_fixed_point()
                message_dict_list = [json.loads(message_json_str) for message_json_str, i in zSet.items() if i == 1]
                #
                foreach_function(message_dict_list)
                #
                time_int = get_millis()
                if snapshot_storage is not None and (time_int - initial_time_int) > snapshot_interval_float * 1000:
                    save_snapshot()
                    #
                    for storage_id_topic_str_tuple, offsets_dict in storage_id_topic_str_tuple_offsets_dict_dict.items():
                        consumer = storage_id_topic_str_tuple_consumer_dict[storage_id_topic_str_tuple]
                        print(f"Committed {offsets_dict} for topic {storage_id_topic_str_tuple[1]}")
                        consumer.commit(offsets_dict)
                #
                await sleep(process_sleep_float)
        except KeyboardInterrupt:
            pass
        finally:
            finally_function()
    #
    def get_latest_offset(message_dict_list, partition_int):
        return next((message_dict["offset"] for message_dict in reversed(message_dict_list) if message_dict["partition"] == partition_int), None)
    #
    def save_snapshot():
        root_topologyNode_bytes = pickle.dumps(root_topologyNode)
        #
        producer = snapshot_storage.producer(snapshot_topic, value_type="bytes")
        producer.produce(root_topologyNode_bytes, key=root_topologyNode.id())
        producer.close()
    #
    def load_snapshot():
        message_dict_list = snapshot_storage.compact(snapshot_topic, value_type="bytes")
        if len(message_dict_list) > 0:
            root_topologyNode_bytes = message_dict_list[0]["value"]
            #
            return pickle.loads(root_topologyNode_bytes)
        else:
            return root_topologyNode
    #
    root_topologyNode = load_snapshot()
    #
    async with TaskGroup() as taskGroup:
        # Create one task for each source of the topology.
        for storage, source_topologyNode, queue in storage_source_topologyNode_queue_tuple_list:
            storage_id = storage.get_id()
            topic_str = source_topologyNode.name()
            consumer = storage_id_topic_str_tuple_consumer_dict[(storage_id, topic_str)]
            taskGroup.create_task(consumer_task(consumer, queue))
        #
        taskGroup.create_task(process())
