import asyncio
import cloudpickle
import hashlib
import threading

from kafi.helpers import get_millis

#

def run_streams(storage_topic_str_tuple_list, root_tn, sink_storage, sink_topic_str, snapshot_storage=None, snapshot_topic=None, **kwargs):
        def _run(stop_thread):
            asyncio.run(streams(storage_topic_str_tuple_list, root_tn, sink_storage, sink_topic_str, snapshot_storage=snapshot_storage, snapshot_topic=snapshot_topic, stop_thread=stop_thread, **kwargs))
        #
        def _stop():
            stop_thread.set()
            thread.join()
        #
        stop_thread = threading.Event()
        thread = threading.Thread(target=_run, args=[stop_thread])
        thread.daemon = True
        thread.start()
        #
        return _stop

async def streams(storage_topic_str_tuple_list, root_tn, sink_storage, sink_topic_str, snapshot_storage=None, snapshot_topic=None, stop_thread=None, **kwargs):
    producer = sink_storage.producer(sink_topic_str, **kwargs)
    #
    def sink_function(message_dict_list):
        producer.produce_list(message_dict_list, **kwargs)
    #
    def finally_function():
        producer.close()
    #
    await streams_function(storage_topic_str_tuple_list, root_tn, sink_function, finally_function, snapshot_storage, snapshot_topic, stop_thread, **kwargs)

async def streams_function(storage_topic_str_tuple_list, root_tn, foreach_function, finally_function, snapshot_storage=None, snapshot_topic=None, stop_thread=None, **kwargs):
    consume_sleep_float = kwargs["consume_sleep"] if "consume_sleep" in kwargs else 0.1
    process_sleep_float = kwargs["process_sleep"] if "process_sleep" in kwargs else 0.1
    snapshot_interval_float = kwargs["snapshot_interval"] if "snapshot_interval" in kwargs else 5.0
    #
    initial_time_int = get_millis()
    #
    def get_latest_offset(message_dict_list, partition_int):
        return next((message_dict["offset"] for message_dict in reversed(message_dict_list) if message_dict["partition"] == partition_int), None)
    #
    last_snapshot_hash_str_list = [None]
    #
    def save_snapshot():
        root_tn_bytes = cloudpickle.dumps(root_tn)
        root_tn_hash_str = hashlib.md5(root_tn_bytes).hexdigest()
        #
        if root_tn_hash_str != last_snapshot_hash_str_list[0]:
            last_snapshot_hash_str_list[0] = root_tn_hash_str
            #
            print("Saving snapshot...")
            producer = snapshot_storage.producer(snapshot_topic, type="bytes", chunk_size_bytes=1000)
            producer.produce(root_tn_bytes, key=root_tn._id_str)
            producer.close()
            print("...saving snapshot done.")

    #
    def load_snapshot():
        message_dict_list = snapshot_storage.compact(snapshot_topic, type="bytes", dechunk=True)
        if len(message_dict_list) > 0:
            root_tn_bytes = message_dict_list[0]["value"]
            #
            root_tn_hash_str = hashlib.md5(root_tn_bytes).hexdigest()
            last_snapshot_hash_str_list[0] = root_tn_hash_str
            #
            print("Loading snaphot...")
            root_tn_ = cloudpickle.loads(root_tn_bytes)
            print("...loading snapshot done.")
            return root_tn_
        else:
            return root_tn
    #
    if snapshot_storage is not None:
        initial_time_int = get_millis()
        #
        root_tn = load_snapshot()
    #
    storage_source_tn_queue_tuple_list = []
    for storage, topic_str in storage_topic_str_tuple_list:
        source_tn = root_tn.get_node_by_name(topic_str)
        #
        queue = asyncio.Queue()
        storage_source_tn_queue_tuple_list.append((storage, source_tn, queue))
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
                    await queue.put(message_dict_list)
                await asyncio.sleep(consume_sleep_float)
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
    #
    async def process():
        try:
            while True and (stop_thread is None or not stop_thread.is_set()):
                storage_id_topic_str_tuple_offsets_dict_dict = {}
                sent_bool = False
                for storage, source_tn, queue in storage_source_tn_queue_tuple_list:
                    if not queue.empty():
                        message_dict_list = await queue.get()
                        #
                        topic_str = source_tn._name_str
                        storage_id_topic_str_tuple = (storage.get_id(), topic_str)
                        partitions_int = storage_id_topic_str_tuple_partitions_int_dict[storage_id_topic_str_tuple]
                        storage_id_topic_str_tuple_offsets_dict_dict[storage_id_topic_str_tuple] = {}
                        for partition_int in range(partitions_int):
                            offset_int = get_latest_offset(message_dict_list, partition_int)
                            # By convention, committed offsets reflect the next message to be consumed, not the last message consumed. (from https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
                            if offset_int is not None:
                                storage_id_topic_str_tuple_offsets_dict_dict[storage_id_topic_str_tuple][partition_int] = offset_int + 1
                        #
                        root_tn.push(source_tn._name_str, message_dict_list)
                        #
                        sent_bool = True
                #
                if sent_bool:
                    message_dict_list, _ = root_tn.step(bag=True)
                    #
                    foreach_function(message_dict_list)
                #
                time_int = get_millis()
                if snapshot_storage is not None and (time_int - initial_time_int) > snapshot_interval_float * 1000:
                    save_snapshot()
                #
                for storage_id_topic_str_tuple, offsets_dict in storage_id_topic_str_tuple_offsets_dict_dict.items():
                    consumer = storage_id_topic_str_tuple_consumer_dict[storage_id_topic_str_tuple]
                    consumer.commit(offsets_dict)
                    print(f"Committed {offsets_dict} for topic {storage_id_topic_str_tuple[1]}.")
                #
                await asyncio.sleep(process_sleep_float)
        except KeyboardInterrupt:
            pass
        finally:
            finally_function()
    #
    async with asyncio.TaskGroup() as taskGroup:
        for storage, source_tn, queue in storage_source_tn_queue_tuple_list:
            storage_id = storage.get_id()
            topic_str = source_tn._name_str
            consumer = storage_id_topic_str_tuple_consumer_dict[(storage_id, topic_str)]
            taskGroup.create_task(consumer_task(consumer, queue))
        #
        taskGroup.create_task(process())
