import asyncio
import cloudpickle
import hashlib
import threading
import zlib

from kafi.helpers import get_millis, copy_kwargs, compress, decompress


def run_streams(storage_topic_str_tuple_list, root_tn, sink_storage, sink_topic_str, checkpoint_storage=None, checkpoint_topic=None, **kwargs):
    """
    Entry point to run the stream processor running in a background daemon thread.
    Returns a stop function to handle a clean shutdown sequence.
    """
    def _run(stop_thread):
        # Initializes and runs the main async entry point within the thread's independent event loop.
        asyncio.run(streams(storage_topic_str_tuple_list, root_tn, sink_storage, sink_topic_str, checkpoint_storage=checkpoint_storage, checkpoint_topic_str=checkpoint_topic, stop_thread_event=stop_thread, **kwargs))
    #
    def _stop():
        # Signals the async loops to stop and blocks until the worker thread exits cleanly.
        stop_thread_event.set()
        thread.join()
    #
    stop_thread_event = threading.Event()
    thread = threading.Thread(target=_run, args=[stop_thread_event])
    thread.daemon = True
    thread.start()
    #
    return _stop


async def streams(storage_topic_str_tuple_list, root_tn, sink_storage, sink_topic_str, checkpoint_storage=None, checkpoint_topic_str=None, stop_thread_event=None, **kwargs):
    """
    Provisions the target sink producer and passes down its callbacks.
    """
    sink_kwargs = copy_kwargs("sink", **kwargs)
    producer = sink_storage.producer(sink_topic_str, **sink_kwargs)
    #
    def sink_function(message_dict_list):
        # Synchronous callback injected into the stream processing logic to pipe outputs to Kafka
        producer.produce_list(message_dict_list)
    #
    def finally_function():
        # Cleanup routine triggered on stream shutdown to safely flush and release the producer
        producer.close()
    #
    await streams_function(storage_topic_str_tuple_list, root_tn, sink_function, finally_function, checkpoint_storage, checkpoint_topic_str, stop_thread_event, **kwargs)


async def streams_function(storage_topic_str_tuple_list, root_tn, foreach_function, finally_function, checkpoint_storage=None, checkpoint_topic_str=None, stop_thread_event=None, **kwargs):
    """
    The core orchestration layer. Manages state loading, instantiates consumers, 
    and handles concurrent data ingestion, stream processing, and fault-tolerant checkpointing.
    """
    checkpoint_interval_float = kwargs["checkpoint_interval"] if "checkpoint_interval" in kwargs else 1.0
    initial_time_int = get_millis()
    #
    last_checkpoint_hash_int = None
    
    def save_checkpoint():
        """
        Serializes and dumps the root TopologyNode object into a compacted storage system.
        Skips writing if the hash matches the previous state to reduce unneeded I/O ops.
        """
        nonlocal last_checkpoint_hash_int
        uncompressed_root_tn_bytes = cloudpickle.dumps(root_tn)
        compressed_root_tn_bytes = compress(uncompressed_root_tn_bytes)
        root_tn_hash_int = zlib.adler32(compressed_root_tn_bytes)
        #
        if root_tn_hash_int != last_checkpoint_hash_int:
            last_checkpoint_hash_int = root_tn_hash_int
            #
            print("Saving checkpoint...")
            producer = checkpoint_storage.producer(checkpoint_topic_str, type="bytes", chunk_size_bytes=1000, **checkpoint_kwargs)
            producer.produce(compressed_root_tn_bytes, key=root_tn._id_str)
            producer.close()
            print("...saving checkpoint done.")

    def load_checkpoint():
        """
        Recovers the root TopologyNode object from the latest checkpoint.
        """
        nonlocal last_checkpoint_hash_int
        message_dict_list = checkpoint_storage.compact(checkpoint_topic_str, value_type="bytes", dechunk=True, **checkpoint_kwargs)
        if len(message_dict_list) > 0:
            compressed_root_tn_bytes = message_dict_list[0]["value"]
            #
            root_tn_hash_int = zlib.adler32(compressed_root_tn_bytes)
            last_checkpoint_hash_int = root_tn_hash_int
            #
            print("Loading checkpoint...")
            uncompressed_root_tn_bytes = decompress(compressed_root_tn_bytes)
            root_tn_ = cloudpickle.loads(uncompressed_root_tn_bytes)
            print("...loading checkpoint done.")
            return root_tn_
        else:
            return root_tn
    #
    # Cold start initialization: Recover root TopologyNode object if a checkpoint backend is provided.
    if checkpoint_storage is not None:
        initial_time_int = get_millis()
        #
        checkpoint_kwargs = copy_kwargs("checkpoint", **kwargs)
        #
        if not checkpoint_storage.exists(checkpoint_topic_str):
            checkpoint_storage.create(checkpoint_topic_str)
        #
        # Offload potentially blocking state deserialization to a threadpool worker
        root_tn = await asyncio.to_thread(load_checkpoint)
    #
    # Instantiate synchronous consumer clients for each source topic.
    storage_id_topic_str_tuple_consumer_dict = {}
    for storage, topic_str in storage_topic_str_tuple_list:
        source_kwargs = copy_kwargs(topic_str, **kwargs)
        consumer = storage.consumer(topic_str, **source_kwargs)
        #
        storage_id = storage.get_id()
        storage_id_topic_str_tuple_consumer_dict[(storage_id, topic_str)] = consumer

    # Shared asynchronous queue: all consumer tasks feed into this single multi-producer structure.
    shared_queue = asyncio.Queue()

    async def consumer_task(storage, source_tn, consumer, queue):
        """
        Background task running concurrently per topic stream.
        Polls Kafka via native blocking calls in dedicated threads and moves batches to async loop space.
        """
        try:
            storage_id = storage.get_id()
            topic_str = source_tn._name_str
            storage_id_topic_str_tuple = (storage_id, topic_str)
            #            
            while True and (stop_thread_event is None or not stop_thread_event.is_set()):
                # Run the blocking synchronous fetch inside an OS thread pool to protect the event loop.
                message_dict_list = await asyncio.to_thread(consumer.consume)
                if message_dict_list:
                    # Enqueue data tagged with storage_id/topic_str pairs.
                    await queue.put((storage_id_topic_str_tuple, message_dict_list))
        except (KeyboardInterrupt, asyncio.CancelledError):
            # Note: Clean-up of client objects omitted here to avoid breaking during inflight processing pipeline drops.
            pass

    async def process():
        """
        Processing loop. Consumes from the async shared queue, processes the deltas, and manages atomic checkpointing.
        """
        nonlocal initial_time_int
        storage_id_topic_str_tuple_offsets_dict_dict = {}
        try:
            while True and (stop_thread_event is None or not stop_thread_event.is_set()):
                try:
                    # Wait for items to arrive. 1.0s timeout ensures periodic exit check and timed commits.
                    storage_id_topic_str_tuple, in_message_dict_list = await asyncio.wait_for(shared_queue.get(), timeout=1.0)
                    # Track committed consumer offsets for transactional commit downstream.
                    if storage_id_topic_str_tuple not in storage_id_topic_str_tuple_offsets_dict_dict:
                        storage_id_topic_str_tuple_offsets_dict_dict[storage_id_topic_str_tuple] = {}
                    # Track last consumer offset for downstream transactional commit.
                    for message_dict in reversed(in_message_dict_list):
                        partition_int = message_dict["partition"]
                        if partition_int not in storage_id_topic_str_tuple_offsets_dict_dict[storage_id_topic_str_tuple]:
                            storage_id_topic_str_tuple_offsets_dict_dict[storage_id_topic_str_tuple][partition_int] = message_dict["offset"]
                    # Push next list of messages consumed from one of the source topics.
                    root_tn.push(storage_id_topic_str_tuple[1], in_message_dict_list)
                    # Process next step and return the latest output batch.
                    out_message_dict_list = root_tn.latest()
                    # Execute foreach_function callback (=produce/call REST API etc.) in a background thread
                    # to safeguard against long-running processing stalls.
                    await asyncio.to_thread(foreach_function, out_message_dict_list)
                except asyncio.TimeoutError:
                    # Catch queue timeout bounds quietly to cycle back into processing/eval state checks
                    pass
                # Checkpoint interval logic: ensures atomic dual-writes of state checkpoints and offsets
                time_int = get_millis()
                if checkpoint_storage is not None and (time_int - initial_time_int) > checkpoint_interval_float * 1000:
                    # Phase 1: Save checkpoint.
                    await asyncio.to_thread(save_checkpoint)
                    # Phase 2: Commit offsets.
                    for storage_id_topic_str_tuple, offsets_dict in storage_id_topic_str_tuple_offsets_dict_dict.items():
                        if offsets_dict:
                            consumer = storage_id_topic_str_tuple_consumer_dict[storage_id_topic_str_tuple]
                            consumer.commit(offsets_dict)
                            print(f"Committed {offsets_dict} for topic {storage_id_topic_str_tuple[1]}.")
                    #
                    storage_id_topic_str_tuple_offsets_dict_dict.clear()
                    initial_time_int = get_millis()
        except KeyboardInterrupt:
            pass
        finally:
            # Trigger e.g. custom producer flush/closure procedures.
            finally_function()

    # Run all consumer routines along with the processing loop within a task group.
    try:
        async with asyncio.TaskGroup() as taskGroup:
            for storage, topic_str in storage_topic_str_tuple_list:
                source_tn = root_tn.get_node_by_name(topic_str)
                storage_id = storage.get_id()
                consumer = storage_id_topic_str_tuple_consumer_dict[(storage_id, topic_str)]
                taskGroup.create_task(consumer_task(storage, source_tn, consumer, shared_queue))
            #
            taskGroup.create_task(process())
    finally:
        # Strict post-termination sequence: close consumer clients only when processing loops have completely stopped
        for consumer in storage_id_topic_str_tuple_consumer_dict.values():
            try:
                consumer.close()
            except Exception:
                pass
