import cloudpickle
import copy
import logging
import sys
import threading
import uuid

from kafi.helpers import get_millis, compress, decompress
from kafi.streams.topologynode import TopologyNode

#

logger = logging.getLogger(__name__)

#

default_checkpoint_interval_float = 1.0

#

streams_prefix_str = "streams_thread_"
checkpoint_suffix_str = "_checkpoint"

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
        return tn

    #
    # Streams main methods
    #

    @staticmethod
    def start_streams(built_tn, checkpoint_storage=None, checkpoint_topic=None, checkpoint_interval=default_checkpoint_interval_float, **kwargs):
        def _run_fun(stop_event):
            Streams.streams(built_tn, checkpoint_storage=checkpoint_storage, checkpoint_topic=checkpoint_topic, checkpoint_interval=checkpoint_interval, stop_event=stop_event, **kwargs)
        #
        def _stop_fun():
            stop_event.set()
            logger.info("Safely stopping Streams...")
            thread.join()
            logger.info("...done.")
        #
        stop_event = threading.Event()
        thread = threading.Thread(target=_run_fun, args=[stop_event])
        thread.daemon = True
        thread.start()
        #
        return _stop_fun

    #

    @staticmethod 
    def streams(built_tn, checkpoint_storage=None, checkpoint_topic=None, checkpoint_interval=default_checkpoint_interval_float, stop_event=None, **kwargs):
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
        Streams.streams_fun(built_tn, sink_str_foreach_fun_finally_fun_tuple_dict, checkpoint_storage=checkpoint_storage, checkpoint_topic=checkpoint_topic, checkpoint_interval=checkpoint_interval, stop_event=stop_event, **kwargs)

    #

    @staticmethod
    def streams_fun(built_tn, sink_str_foreach_fun_finally_fun_tuple_dict, checkpoint_storage=None, checkpoint_topic=None, checkpoint_interval=default_checkpoint_interval_float, stop_event=None, **kwargs):
        if not built_tn._sink_str_list:
            raise Exception("No terminal sink.")
        #
        checkpoint_topic_str = checkpoint_topic
        checkpoint_interval_float = checkpoint_interval
        #
        outputs_int = 0
        #
        def progress_fun(b_tn, source_str_offsets_dict_dict, time_int):
            sys.stdout.write(f"\rUptime: {(time_int - initial_time_int) / 1000:.3f}s, State size: {b_tn.size() / 1024:.2f} KB, Source offsets: {source_str_offsets_dict_dict}, Sink outputs: {outputs_int}")
            sys.stdout.flush()
        #
        step_fun = kwargs["step_fun"] if "step_fun" in kwargs else lambda _b_tn, _source_str_offsets_dict_dict: None
        #
        initial_time_int = get_millis()
        #
        def save_checkpoint(source_str_offsets_dict_dict):
            checkpoint_dict = {"evaluator": built_tn._evaluator,
                               "offsets": source_str_offsets_dict_dict}
            uncompressed_checkpoint_bytes = cloudpickle.dumps(checkpoint_dict)
            compressed_checkpoint_bytes = compress(uncompressed_checkpoint_bytes)
            #
            logger.info("Saving checkpoint...")
            chunk_size_bytes_int = kwargs["chunk_size_bytes"] if "chunk_size_bytes" in kwargs else 1000
            producer = checkpoint_storage.producer(checkpoint_topic_str, type="bytes", chunk_size_bytes=chunk_size_bytes_int, **kwargs)
            producer.produce(compressed_checkpoint_bytes, key=built_tn.get_id())
            producer.close()
            logger.info("...saving checkpoint done (%d KB compressed, %d uncompressed).", len(compressed_checkpoint_bytes) / 1024, len(uncompressed_checkpoint_bytes) / 1024)
        #
        def load_checkpoint(built_tn):
            checkpoint_group_str = group_str + checkpoint_suffix_str
            checkpoint_kwargs = kwargs.copy()
            checkpoint_kwargs["group"] = checkpoint_group_str
            #
            logger.debug("Checkpoint consumer group ('%s') offsets for topic '%s': %s", checkpoint_group_str, checkpoint_topic_str, checkpoint_storage.group_offsets(checkpoint_group_str).get(checkpoint_group_str, {}).get(checkpoint_topic_str, {}))
            #
            m_list = checkpoint_storage.compact(checkpoint_topic_str, value_type="bytes", dechunk=True, **checkpoint_kwargs)
            #
            source_str_offsets_dict_dict = None
            if len(m_list) > 0:
                compressed_checkpoint_bytes = m_list[0]["value"]
                #
                logger.info("Loading checkpoint...")
                uncompressed_checkpoint_bytes = decompress(compressed_checkpoint_bytes)
                checkpoint_dict = cloudpickle.loads(uncompressed_checkpoint_bytes)
                built_tn._evaluator = checkpoint_dict["evaluator"]
                source_str_offsets_dict_dict = checkpoint_dict["offsets"]
                logger.info("...loading checkpoint done (%d KB compressed, %d uncompressed).", len(compressed_checkpoint_bytes) / 1024, len(uncompressed_checkpoint_bytes) / 1024)
            #
            return source_str_offsets_dict_dict
        #
        group_str = kwargs["group"] if "group" in kwargs else f"streams_{get_millis()}"
        #
        source_str_topic_dict_dict = built_tn.get_source_str_topic_dict_dict()
        #
        source_str_offsets_dict_dict = None
        if checkpoint_storage is not None:
            initial_time_int = get_millis()
            #
            if checkpoint_storage.exists(checkpoint_topic_str):
                source_str_offsets_dict_dict = load_checkpoint(built_tn)
            else:
                checkpoint_storage.create(checkpoint_topic_str)
        #
        source_str_consumer_dict = {}
        for source_str, topic_dict in source_str_topic_dict_dict.items():
            storage = topic_dict["storage"]
            topic_str = topic_dict["topic"]
            source_kwargs = topic_dict["kwargs"]
            #
            source_kwargs["group"] = group_str
            #
            logger.debug("Source consumer group ('%s') offsets for topic '%s': %s", group_str, topic_str, storage.group_offsets(group_str).get(group_str, {}).get(topic_str, {}))
            #
            if checkpoint_storage is not None and source_kwargs.get("enable_auto_commit", storage.enable_auto_commit()):
                logger.warning("Checkpointing enabled but enable_auto_commit is True for source '%s': checkpoint/offset consistency guarantee does not hold.", source_str)
            #
            if source_str_offsets_dict_dict is not None:
                if source_str in source_str_offsets_dict_dict:
                    source_kwargs["offsets"] = source_str_offsets_dict_dict[source_str]
                    #
                    logger.debug("Source consumer group offsets for topic '%s' overridden by checkpoint offsets: %s", source_str, source_str_offsets_dict_dict[source_str])
            #
            consumer = storage.consumer(topic_str, **source_kwargs)
            #
            source_str_consumer_dict[source_str] = consumer
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
        source_str_m_list_dict = {}
        #
        source_str_offsets_dict_dict = {}
        last_committed_source_str_offsets_dict_dict = {}
        #
        try:
            while (stop_event is None or not stop_event.is_set()):
                for source_str, consumer in source_str_consumer_dict.items():
                    m_list = consumer.consume()
                    #
                    source_str_m_list_dict[source_str] = m_list
                #
                for source_str, source_m_list in source_str_m_list_dict.items():
                    source_partitions_int = source_str_partitions_int_dict[source_str]
                    for partition_int in range(source_partitions_int):
                        offset_int = next((m["offset"] for m in reversed(source_m_list) if m["partition"] ==    partition_int), None)
                        if offset_int is not None:
                            source_str_offsets_dict_dict.setdefault(source_str, {})[partition_int] = offset_int + 1
                #
                built_tn.push(source_str_m_list_dict)
                #
                sink_str_sink_m_list_dict = built_tn.latest()
                #
                step_fun(built_tn, source_str_offsets_dict_dict)
                #
                for sink_str, (foreach_fun, _) in sink_str_foreach_fun_finally_fun_tuple_dict.items():
                    sink_m_list = sink_str_sink_m_list_dict.get(sink_str, [])
                    if sink_m_list != []:
                        foreach_fun(sink_m_list)
                        #
                        outputs_int += len(sink_m_list)
                #
                time_int = get_millis()
                #
                if kwargs.get("progress", False):
                    progress_fun(built_tn, source_str_offsets_dict_dict, time_int)
                #
                if source_str_offsets_dict_dict and source_str_offsets_dict_dict != last_committed_source_str_offsets_dict_dict:
                    if checkpoint_storage is not None and (time_int - initial_time_int) > checkpoint_interval_float * 1000:
                        save_checkpoint(source_str_offsets_dict_dict)
                        #
                        for source_str, offsets_dict in source_str_offsets_dict_dict.items():
                            if offsets_dict:
                                consumer = source_str_consumer_dict[source_str]
                                consumer.commit(offsets_dict)
                                logger.info("Committed %s for source %s.", offsets_dict, source_str)
                        #
                        last_committed_source_str_offsets_dict_dict = copy.deepcopy(source_str_offsets_dict_dict)
                        #
                        initial_time_int = get_millis()
        except Exception:
            logger.exception("Exception in streams loop")
            raise
        finally:
            for sink_str, (_, finally_fun) in sink_str_foreach_fun_finally_fun_tuple_dict.items():
                try:
                    finally_fun()
                except Exception:
                    logger.exception("Esception in finally_fun() for sink '%s'", sink_str)
            #
            for consumer in source_str_consumer_dict.values():
                try:
                    consumer.close()
                except Exception:
                    logger.exception("Exception in clean up.")
    ###
    # Sources/sinks helpers
    ###

    def get_source_str_topic_dict_dict(self):
        return {source_str: source_streams._topic_dict for source_str, source_streams in self.get_source_nodes().items()}
    
    def get_sink_str_topic_dict_dict(self):
        return {sink_str: sink_streams._topic_dict for sink_str, sink_streams in self.get_sink_nodes().items()}

    ###
    # Thread helpers
    ###

    @staticmethod
    def threads():
        thread_list = threading.enumerate()
        #
        streams_thread_list = [thread for thread in thread_list if thread.name.startswith(streams_prefix_str)]
        #
        return streams_thread_list
    
    # Exclude the Storage object (_topic_dict["storage"]) from pickling.
    # Avoids: TypeError: cannot pickle 'AdminClient' object
    # def __getstate__(self):
    #     state = self.__dict__.copy()
    #     if "_topic_dict" in state and state["_topic_dict"]:
    #         topic_dict = state["_topic_dict"].copy()
    #         topic_dict.pop("storage", None)
    #         state["_topic_dict"] = topic_dict
    #     return state

    def __getstate__(self):
        state = self.__dict__.copy()
        #
        state.pop("step_fun", None)
        #
        if "_topic_dict" in state and state["_topic_dict"]:
            topic_dict = state["_topic_dict"].copy()
            topic_dict.pop("storage", None)
            state["_topic_dict"] = topic_dict
        return state
    