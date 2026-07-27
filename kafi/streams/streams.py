import asyncio
import cloudpickle
import threading
import traceback
import uuid
import zlib

from kafi.helpers import get_millis, compress, decompress
from kafi.streams.topologynode import TopologyNode

#

default_checkpoint_interval_float = 1.0

#

streams_prefix_str = "streams_thread_"

def create_name():
    return f"{streams_prefix_str}{str(uuid.uuid4())}"

#

class Streams(TopologyNode):
    #
    # Sources and sinks
    #

    @staticmethod
    def source(storage, source_str, topic_str=None, **kwargs):
        tn = TopologyNode.source(source_str)
        tn.__class__ = Streams
        #
        tn._topic_dict = {"storage": storage,
                          "topic": source_str if topic_str is None else topic_str,
                          "kwargs": kwargs}
        #
        return tn
    
    def sink(self, storage, sink_str, topic_str=None, **kwargs):
        tn = super().sink(sink_str)
        #
        tn._topic_dict = {"storage": storage,
                          "topic": sink_str if topic_str is None else topic_str,
                          "kwargs": kwargs}
        #
        return self

    #
    # Streams main methods
    #

    @staticmethod
    def start_streams_task(built_tn, checkpoint_storage=None, checkpoint_topic=None, checkpoint_interval=default_checkpoint_interval_float, **kwargs):
        stop_event = threading.Event()
        #
        task = asyncio.create_task(Streams.streams(built_tn, checkpoint_storage=checkpoint_storage, checkpoint_topic=checkpoint_topic, checkpoint_interval=checkpoint_interval, stop_event=stop_event, **kwargs), name=create_name())
        #
        async def _stop_fun():
            print("Safely stopping Streams...")
            stop_event.set()
            await task
            print("...done.")
        #
        return _stop_fun

    #

    @staticmethod
    def start_streams_thread(built_tn, checkpoint_storage=None, checkpoint_topic=None, checkpoint_interval=default_checkpoint_interval_float, **kwargs):
        def _run_fun(stop_event):
            asyncio.run(Streams.streams(built_tn, checkpoint_storage=checkpoint_storage, checkpoint_topic=checkpoint_topic, checkpoint_interval=checkpoint_interval, stop_event=stop_event, **kwargs))
        #
        def _stop_fun():
            stop_event.set()
            print("Safely stopping Streams...")
            thread.join()
            print("...done.")
        #
        stop_event = threading.Event()
        thread = threading.Thread(target=_run_fun, args=[stop_event])
        thread.daemon = True
        thread.start()
        #
        return _stop_fun

    #

    @staticmethod 
    async def streams(built_tn, checkpoint_storage=None, checkpoint_topic=None, checkpoint_interval=default_checkpoint_interval_float, stop_event=None, **kwargs):
        if threading.current_thread() is not None:
            threading.current_thread().name = create_name()
        #
        sink_str_topic_dict_dict = built_tn.get_sink_str_topic_dict_dict()
        #
        sink_str_producer_dict = {}
        for sink_str, topic_dict in sink_str_topic_dict_dict.items():
            storage = topic_dict["storage"]
            topic_str = topic_dict["topic"]
            sink_kwargs = topic_dict["kwargs"]
            #
            producer = storage.producer(topic_str, **sink_kwargs)
            #
            sink_str_producer_dict[sink_str] = producer
        #
        def get_foreach_fun(sink_str):
            producer = sink_str_producer_dict[sink_str]
            return producer.produce_list
        #
        def get_finally_fun(sink_str):
            producer = sink_str_producer_dict[sink_str]
            return producer.close
        #
        sink_str_foreach_fun_finally_fun_tuple_dict = {sink_str: (get_foreach_fun(sink_str), get_finally_fun(sink_str)) for sink_str, _ in sink_str_topic_dict_dict.items()}
        #
        await Streams.streams_fun(built_tn, sink_str_foreach_fun_finally_fun_tuple_dict, checkpoint_storage=checkpoint_storage, checkpoint_topic=checkpoint_topic, checkpoint_interval=checkpoint_interval, stop_event=stop_event, **kwargs)

    #

    @staticmethod
    async def streams_fun(built_tn, sink_str_foreach_fun_finally_fun_tuple_dict, checkpoint_storage=None, checkpoint_topic=None, checkpoint_interval=default_checkpoint_interval_float, stop_event=None, **kwargs):
        checkpoint_topic_str = checkpoint_topic
        checkpoint_interval_float = checkpoint_interval
        #
        step_fun = kwargs["step_fun"] if "step_fun" in kwargs else lambda _: None
        #
        initial_time_int = get_millis()
        #
        last_checkpoint_hash_int = None
        #
        def save_checkpoint():
            nonlocal last_checkpoint_hash_int
            uncompressed_checkpoint_bytes = cloudpickle.dumps(built_tn._evaluator)
            compressed_checkpoint_bytes = compress(uncompressed_checkpoint_bytes)
            checkpoint_hash_int = zlib.adler32(compressed_checkpoint_bytes)
            #
            if checkpoint_hash_int != last_checkpoint_hash_int:
                last_checkpoint_hash_int = checkpoint_hash_int
                #
                print("Saving checkpoint...")
                chunk_size_bytes_int = kwargs["chunk_size_bytes"] if "chunk_size_bytes" in kwargs else 1000
                producer = checkpoint_storage.producer(checkpoint_topic_str, type="bytes", chunk_size_bytes=chunk_size_bytes_int, **kwargs)
                producer.produce(compressed_checkpoint_bytes, key=built_tn.get_id())
                producer.close()
                print("...saving checkpoint done.")

        def load_checkpoint(built_tn):
            nonlocal last_checkpoint_hash_int
            #
            checkpoint_kwargs = kwargs.copy()
            group_str = checkpoint_kwargs.get("group", None)
            if group_str is not None:
                checkpoint_group_str = group_str + "_checkpoint"
                checkpoint_kwargs["group"] = checkpoint_group_str
            #
            print(f"Checkpoint consumer group offsets for topic {checkpoint_topic_str}: {checkpoint_storage.group_offsets(checkpoint_group_str).get(checkpoint_group_str, {}).get(checkpoint_topic_str, {})}")
            #
            m_list = checkpoint_storage.compact(checkpoint_topic_str, value_type="bytes", dechunk=True, **checkpoint_kwargs)
            if len(m_list) > 0:
                compressed_checkpoint_bytes = m_list[0]["value"]
                #
                checkpoint_hash_int = zlib.adler32(compressed_checkpoint_bytes)
                last_checkpoint_hash_int = checkpoint_hash_int
                #
                print("Loading checkpoint...")
                uncompressed_checkpoint_bytes = decompress(compressed_checkpoint_bytes)
                evaluator = cloudpickle.loads(uncompressed_checkpoint_bytes)
                built_tn._evaluator = evaluator
                print("...loading checkpoint done.")
        #
        group_str = kwargs["group"] if "group" in kwargs else f"streams_{get_millis()}"
        #
        source_str_topic_dict_dict = built_tn.get_source_str_topic_dict_dict()
        #
        source_str_consumer_dict = {}
        for source_str, topic_dict in source_str_topic_dict_dict.items():
            storage = topic_dict["storage"]
            topic_str = topic_dict["topic"]
            source_kwargs = topic_dict["kwargs"]
            #
            source_kwargs["group"] = group_str
            #
            print(f"Source consumer group offsets for topic {topic_str}: {storage.group_offsets(group_str).get(group_str, {}).get(topic_str, {})}")
            consumer = storage.consumer(topic_str, **source_kwargs)
            #
            source_str_consumer_dict[source_str] = consumer
        #
        source_str_m_list_tuple_queue = asyncio.Queue()
        #
        source_str_partitions_int_dict = {}
        for source_str, topic_dict in source_str_topic_dict_dict.items():
            storage = topic_dict["storage"]
            topic_str = topic_dict["topic"]
            #
            partitions_int = storage.partitions(topic_str)[topic_str]
            #
            source_str_partitions_int_dict[source_str] = partitions_int
        #
        async def consumer_task(source_str, consumer):
            try:
                while True and (stop_event is None or not stop_event.is_set()):
                    m_list = await asyncio.to_thread(consumer.consume)
                    if m_list:
                        await source_str_m_list_tuple_queue.put((source_str, m_list))
            except (KeyboardInterrupt, asyncio.CancelledError):
                pass
            except Exception:
                traceback.print_exc()
        #
        async def process():
            nonlocal initial_time_int
            source_str_offsets_dict_dict = {}
            try:
                while (stop_event is None or not stop_event.is_set()):
                    try:
                        source_str, source_m_list = await asyncio.wait_for(source_str_m_list_tuple_queue.get(), timeout=1.0)
                        #
                        for partition_int in range(partitions_int):
                            offset_int = next((m["offset"] for m in reversed(source_m_list) if m["partition"] == partition_int), None)
                            if offset_int is not None:
                                source_str_offsets_dict_dict.setdefault(source_str, {})[partition_int] = offset_int
                        #
                        built_tn.push(source_str, source_m_list)
                        #
                        sink_str_sink_m_list_dict = built_tn.latest()
                        #
                        step_fun(built_tn)
                        #
                        for sink_str, (foreach_fun, _) in sink_str_foreach_fun_finally_fun_tuple_dict.items():
                            sink_m_list = sink_str_sink_m_list_dict.get(sink_str, [])
                            if sink_m_list != []:
                                await asyncio.to_thread(foreach_fun, sink_m_list)
                    except asyncio.TimeoutError:
                        pass
                    time_int = get_millis()
                    if checkpoint_storage is not None and (time_int - initial_time_int) > checkpoint_interval_float * 1000:
                        await asyncio.to_thread(save_checkpoint)
                        #
                        for source_str, offsets_dict in source_str_offsets_dict_dict.items():
                            if offsets_dict:
                                consumer = source_str_consumer_dict[source_str]
                                consumer.commit(offsets_dict)
                                print(f"Committed {offsets_dict} for source {source_str}.")
                        #
                        source_str_offsets_dict_dict.clear()
                        initial_time_int = get_millis()
            except KeyboardInterrupt:
                pass
            except Exception:
                traceback.print_exc()
            finally:
                for (_, finally_fun) in sink_str_foreach_fun_finally_fun_tuple_dict.values():
                    await asyncio.to_thread(finally_fun)
        #
        if checkpoint_storage is not None:
            initial_time_int = get_millis()
            #
            if checkpoint_storage.exists(checkpoint_topic_str):
                load_checkpoint(built_tn)
            else:
                checkpoint_storage.create(checkpoint_topic_str)
        #
        try:
            async with asyncio.TaskGroup() as taskGroup:
                for source_str, consumer in source_str_consumer_dict.items():
                    taskGroup.create_task(consumer_task(source_str, consumer))
                #
                taskGroup.create_task(process())
        finally:
            for consumer in source_str_consumer_dict.values():
                try:
                    consumer.close()
                except Exception:
                    traceback.print_exc()
    ###
    # Sources/sinks helpers
    ###

    def get_source_str_topic_dict_dict(self):
        return {source_str: source_streams._topic_dict for source_str, source_streams in self.get_source_nodes().items()}
    
    def get_sink_str_topic_dict_dict(self):
        return {sink_str: sink_streams._topic_dict for sink_str, sink_streams in self.get_sink_nodes().items()}

    ###
    # Thread/task helpers
    ###

    @staticmethod
    def threads():
        thread_list = threading.enumerate()
        #
        streams_thread_list = [thread for thread in thread_list if thread.name.startswith(streams_prefix_str)]
        #
        return streams_thread_list

    @staticmethod
    async def tasks():
        task_set = asyncio.all_tasks()
        #
        streams_task_list = [task for task in task_set if task.get_name().startswith(streams_prefix_str)]
        #
        return streams_task_list
    
    @staticmethod
    async def cancel_all_tasks():
        streams_task_list = await Streams.tasks()
        #
        for streams_task in streams_task_list:
            streams_task.cancel()
        #
        streams_task_list = await Streams.tasks()
        #
        return streams_task_list

    # Exclude the Storage object (_topic_dict["storage"]) from pickling.
    # Avoids: TypeError: cannot pickle 'AdminClient' object
    def __getstate__(self):
        state = self.__dict__.copy()
        if "_topic_dict" in state and state["_topic_dict"]:
            topic_dict = state["_topic_dict"].copy()
            topic_dict.pop("storage", None)
            state["_topic_dict"] = topic_dict
        return state
