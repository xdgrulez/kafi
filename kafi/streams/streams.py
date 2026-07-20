import asyncio
import cloudpickle
import threading
import traceback
import zlib

from kafi.helpers import get_millis, compress, decompress
from kafi.streams.topologynode import TopologyNode

#

default_checkpoint_interval_float = 1.0

#

def start_streams_task(built_tn, checkpoint_storage=None, checkpoint_topic=None, checkpoint_interval=default_checkpoint_interval_float, **kwargs):
    stop_event = threading.Event()
    #
    task = asyncio.create_task(streams(built_tn, checkpoint_storage=checkpoint_storage, checkpoint_topic=checkpoint_topic, checkpoint_interval=checkpoint_interval, stop_event=stop_event, **kwargs))
    #
    async def _stop():
        print("Safely stopping streams...")
        stop_event.set()
        await task
        print("...done.")
    #
    return _stop

#

def start_streams(built_tn, checkpoint_storage=None, checkpoint_topic=None, checkpoint_interval=default_checkpoint_interval_float, **kwargs):
    """
    Entry point to run the stream processor running in a background daemon thread.
    Returns a stop function to handle a clean shutdown sequence.
    """
    def _run(stop_event):
        # Initializes and runs the main async entry point within the thread's independent event loop.
        asyncio.run(streams(built_tn, checkpoint_storage=checkpoint_storage, checkpoint_topic=checkpoint_topic, checkpoint_interval=checkpoint_interval, stop_event=stop_event, **kwargs))
    #
    def _stop():
        # Signals the async loops to stop and blocks until the worker thread exits cleanly.
        stop_event.set()
        print("Safely stopping streams...")
        thread.join()
        print("...done.")
    #
    stop_event = threading.Event()
    thread = threading.Thread(target=_run, args=[stop_event])
    thread.daemon = True
    thread.start()
    #
    return _stop

#

async def streams(built_tn, checkpoint_storage=None, checkpoint_topic=None, checkpoint_interval=default_checkpoint_interval_float, stop_event=None, **kwargs):
    """
    Provisions the sink producers and passes down its callbacks.
    """
    sink_str_topic_dict_dict = get_sink_str_topic_dict_dict(built_tn)
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
    await streams_fun(built_tn, sink_str_foreach_fun_finally_fun_tuple_dict, checkpoint_storage=checkpoint_storage, checkpoint_topic=checkpoint_topic, checkpoint_interval=checkpoint_interval, stop_event=stop_event, **kwargs)


async def streams_fun(built_tn, sink_str_foreach_fun_finally_fun_tuple_dict, checkpoint_storage=None, checkpoint_topic=None, checkpoint_interval=default_checkpoint_interval_float, stop_event=None, **kwargs):
    """
    The core orchestration layer. Manages state loading, instantiates consumers, 
    and handles concurrent data ingestion, stream processing, and fault-tolerant checkpointing.
    """
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
        """
        Serializes and dumps built_tn into a compacted storage system.
        Skips writing if the hash matches the previous state to reduce unneeded I/O ops.
        """
        nonlocal last_checkpoint_hash_int
        uncompressed_checkpoint_bytes = cloudpickle.dumps(built_tn)
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
        """
        Recovers the build_tn object from the latest checkpoint.
        """
        nonlocal last_checkpoint_hash_int
        message_dict_list = checkpoint_storage.compact(checkpoint_topic_str, value_type="bytes", dechunk=True, **kwargs)
        if len(message_dict_list) > 0:
            compressed_checkpoint_bytes = message_dict_list[0]["value"]
            #
            checkpoint_hash_int = zlib.adler32(compressed_checkpoint_bytes)
            last_checkpoint_hash_int = checkpoint_hash_int
            #
            print("Loading checkpoint...")
            uncompressed_checkpoint_bytes = decompress(compressed_checkpoint_bytes)
            built_tn = cloudpickle.loads(uncompressed_checkpoint_bytes)
            print("...loading checkpoint done.")
            return built_tn
        else:
            return built_tn
    #
    # Create consumer clients for each source topic.
    group_str = kwargs["group"] if "group" in kwargs else f"streams_{get_millis()}"
    #
    source_str_topic_dict_dict = get_source_str_topic_dict_dict(built_tn)
    clear_topic_dict(built_tn)
    #
    source_str_consumer_dict = {}
    for source_str, topic_dict in source_str_topic_dict_dict.items():
        storage = topic_dict["storage"]
        topic_str = topic_dict["topic"]
        source_kwargs = topic_dict["kwargs"]
        #
        source_kwargs["group"] = group_str
        consumer = storage.consumer(topic_str, **source_kwargs)
        #
        source_str_consumer_dict[source_str] = consumer
    #
    # Create shared source queue.
    source_str_message_dict_list_tuple_queue = asyncio.Queue()
    #
    # Create cache for source topic partition counts.
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
        """
        Background task running concurrently per topic stream.
        Polls Kafka via native blocking calls in dedicated threads and moves batches to async loop space.
        """
        try:
            while True and (stop_event is None or not stop_event.is_set()):
                # Run the blocking synchronous fetch inside an OS thread pool to protect the event loop.
                message_dict_list = await asyncio.to_thread(consumer.consume)
                if message_dict_list:
                    # Enqueue data tagged with storage_id/topic_str pairs.
                    await source_str_message_dict_list_tuple_queue.put((source_str, message_dict_list))
        except (KeyboardInterrupt, asyncio.CancelledError):
            # Note: Clean-up of client objects omitted here to avoid breaking during inflight processing pipeline drops.
            pass
        except Exception:
            traceback.print_exc()
    #
    async def process():
        """
        Processing loop. Consumes from the async shared queue, processes the deltas, and manages atomic checkpointing.
        """
        nonlocal initial_time_int
        source_str_offsets_dict_dict = {}
        try:
            while (stop_event is None or not stop_event.is_set()):
                try:
                    # Wait for items to arrive. 1.0s timeout ensures periodic exit check and timed commits.
                    source_str, source_message_dict_list = await asyncio.wait_for(source_str_message_dict_list_tuple_queue.get(), timeout=1.0)
                    #
                    # Get latest offsets.
                    for partition_int in range(partitions_int):
                        offset_int = get_latest_offset(source_message_dict_list, partition_int)
                        if offset_int is not None:
                            source_str_offsets_dict_dict.setdefault(source_str, {})[partition_int] = offset_int
                    #
                    # Push the latest source messages.
                    built_tn.push(source_str, source_message_dict_list)
                    #
                    # Process the next step and return the latest sink messages.
                    sink_str_sink_message_dict_list_dict = built_tn.latest()
                    #
                    step_fun(built_tn)
                    #
                    for sink_str, (foreach_fun, _) in sink_str_foreach_fun_finally_fun_tuple_dict.items():
                        # Call foreach function for each sink (in a background thread).
                        sink_message_dict_list = sink_str_sink_message_dict_list_dict.get(sink_str, [])
                        if sink_message_dict_list != []:
                            await asyncio.to_thread(foreach_fun, sink_message_dict_list)
                except asyncio.TimeoutError:
                    # Catch queue timeout bounds quietly to cycle back into processing/eval state checks.
                    pass
                # Checkpoint interval logic: ensures atomic dual-writes of state checkpoints and offsets.
                time_int = get_millis()
                if checkpoint_storage is not None and (time_int - initial_time_int) > checkpoint_interval_float * 1000:
                    # Phase 1: Save checkpoint.
                    await asyncio.to_thread(save_checkpoint)
                    # Phase 2: Commit offsets.
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
            # Trigger e.g. custom producer flush/closure procedures.
            for (_, finally_fun) in sink_str_foreach_fun_finally_fun_tuple_dict.values():
                finally_fun()
    #
    # Cold start initialization: Recover built_tn if a checkpoint backend is provided.
    if checkpoint_storage is not None:
        initial_time_int = get_millis()
        #
        if not checkpoint_storage.exists(checkpoint_topic_str):
            checkpoint_storage.create(checkpoint_topic_str)
        #
        # Offload potentially blocking state deserialization to a threadpool worker
        built_tn = await asyncio.to_thread(load_checkpoint, built_tn)
    #
    # Run all consumer routines along with the processing loop within a task group.
    try:
        async with asyncio.TaskGroup() as taskGroup:
            for source_str, consumer in source_str_consumer_dict.items():
                taskGroup.create_task(consumer_task(source_str, consumer))
            #
            taskGroup.create_task(process())
    finally:
        # Strict post-termination sequence: close consumer clients only when processing loops have completely stopped
        for consumer in source_str_consumer_dict.values():
            try:
                consumer.close()
            except Exception:
                traceback.print_exc()
    
# Streams class (to augment sources and sinks with storage/topic/kwargs information)

class Streams(TopologyNode):
    @staticmethod
    def source(source_str, storage, topic_str=None, **kwargs):
        tn = TopologyNode.source(source_str)
        tn.__class__ = Streams
        #
        tn._topic_dict = {"storage": storage,
                          "topic": source_str if topic_str is None else topic_str,
                          "kwargs": kwargs}
        #
        return tn
    
    def sink(self, sink_str, storage, topic_str=None, **kwargs):
        tn = super().sink(sink_str)
        #
        tn._topic_dict = {"storage": storage,
                          "topic": sink_str if topic_str is None else topic_str,
                          "kwargs": kwargs}
        #
        return self

# Helpers

def get_latest_offset(message_dict_list, partition_int):
    return next((message_dict["offset"] for message_dict in reversed(message_dict_list) if message_dict["partition"] == partition_int), None)

def get_source_str_topic_dict_dict(built_tn):
    source_str_topic_dict_dict = {source_str: source_streams._topic_dict for source_str, source_streams in built_tn.get_source_nodes().items()}
    #
    return source_str_topic_dict_dict

def get_sink_str_topic_dict_dict(built_tn):
    sink_str_topic_dict_dict = {sink_str: sink_streams._topic_dict for sink_str, sink_streams in built_tn.get_sink_nodes().items()}
    #
    return sink_str_topic_dict_dict

def clear_topic_dict(built_tn):
    def _clear_topic_dict(tn):
        if hasattr(tn, "_topic_dict") and tn._topic_dict is not None:
            tn._topic_dict = None
    #
    built_tn._foreach_bu(_clear_topic_dict)
