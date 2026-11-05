from kafi.dechunker import Dechunker
from kafi.helpers import get_millis

import copy, sys, time

from tqdm.auto import tqdm

# Constants

ALL_MESSAGES = -1
OFFSET_INVALID = -1001

#

class StorageConsumer(Dechunker):
    def __init__(self, storage_obj, topic, **kwargs):
        """Resolve topics, start/end offsets, partitions, key/value types and group for a new consumer.

        Args:
            storage_obj: owning storage instance
            topic: a topic or a list of topics to subscribe to (str or list(str))
            **kwargs: "offsets"/"ts", "end_offsets"/"end_ts", "partitions", "group", "config", "enable_auto_commit", "isolation.level", key/value type overrides"""
        self.storage_obj = storage_obj
        #
        # Get topics to subscribe to.
        if isinstance(topic, str):
            self.topic_str_list = [topic]
        elif isinstance(topic, list) and all(isinstance(topic_str, str) for topic_str in topic):
            self.topic_str_list = topic
        else:
            raise Exception("Can only subscribe to an individual topic or a list of topics.")
        #
        # Get dictionary mapping the topics to their respective number of partitions.
        self.topic_str_partitions_int_dict = self.storage_obj.partitions(self.topic_str_list)
        for topic_str in self.topic_str_list:
            if topic_str not in self.topic_str_partitions_int_dict:
                raise Exception(f"Topic \"{topic_str}\" does not exist.")
        #
        # Dictionary mapping the topics to their respective current watermarks.
        self.topic_str_partition_int_offsets_tuple_dict_dict = self.storage_obj.watermarks(self.topic_str_list)
        #
        # Dictionary mapping the topics to their respective assign (=start) offsets.
        #   These are the offsets used in the "inner" implementations (consume_impl(), but also subscribe() for Cluster and RestProxy)...
        #   ...1) initialize with OFFSET_INVALID (=-1001)
        self.topic_str_assign_offsets_dict_dict = {topic_str: {partition_int: OFFSET_INVALID for partition_int in range(partitions_int)} for topic_str, partitions_int in self.topic_str_partitions_int_dict.items()}
        #   ...2) apply the kwargs "offsets"/"ts" args if available.
        kwargs_topic_str_offsets_dict_dict = self.get_offsets_from_kwargs("offsets", "ts", **kwargs)
        for topic_str, offsets_dict in kwargs_topic_str_offsets_dict_dict.items():
            for partition_int, offset_int in offsets_dict.items():
                self.topic_str_assign_offsets_dict_dict[topic_str][partition_int] = offset_int
        #
        # Dictionary mapping the topics to their respective commit offsets.
        #   These are the offsets used in the "outer" foldl() loop.
        self.topic_str_commit_offsets_dict_dict = copy.deepcopy(self.topic_str_assign_offsets_dict_dict)
        #
        # Get dict mapping the topics to their respective end offsets...
        # ...1) initialize with sys.maxsize
        self.topic_str_end_offsets_dict_dict = {topic_str: {partition_int: sys.maxsize for partition_int in range(self.topic_str_partitions_int_dict[topic_str])} for topic_str in self.topic_str_list}
        # ...2) apply the kwargs "end_offsets"/"end_ts" if available.
        kwargs_topic_str_end_offsets_dict_dict = self.get_offsets_from_kwargs("end_offsets", "end_ts", **kwargs)
        for topic_str, offsets_dict in kwargs_topic_str_end_offsets_dict_dict.items():
            for partition_int, offset_int in offsets_dict.items():
                self.topic_str_end_offsets_dict_dict[topic_str][partition_int] = offset_int
        #
        # Get partitions to assign for the subscribed topics.
        self.topic_str_partition_int_list_dict = self.get_partitions_from_kwargs("partitions", **kwargs)
        #
        # Get key and value types.
        (self.topic_str_key_type_str_dict, self.topic_str_value_type_str_dict) = self.get_key_value_types_from_kwargs(**kwargs)
        #
        # Get group.
        self.group_str = self.get_group_str_from_kwargs(**kwargs)
        #
        # Configure.
        self.consumer_config_dict = {}
        self.consumer_config_dict["auto.offset.reset"] = storage_obj.auto_offset_reset()
        if "config" in kwargs:
            for key_str, value in kwargs["config"].items():
                self.consumer_config_dict[key_str] = value
        #
        self.enable_auto_commit_bool = kwargs["enable_auto_commit"] if "enable_auto_commit" in kwargs else storage_obj.enable_auto_commit()
        #
        self.isolation_level_str = kwargs["isolation.level"] if "isolation.level" in kwargs else storage_obj.isolation_level()
        #
        self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict = {}
        #
        super().__init__(storage_obj.schema_registry_config_dict, **kwargs)

    #

    def foldl(self, foldl_fun, initial_acc, n=ALL_MESSAGES, commit_after_processing=None, **kwargs):
        """Consume and fold over messages (with dechunking, deserialization, offset tracking, commits). Stops either after no more messages could be consumed *and* for all topics and their partitions, the high watermark has been reached, the maximum number of messages have been consumed over all topics, or break_fun is triggered. 

        Args:
            foldl_fun: (acc, m) -> new acc - fold left function - takes the current accumulator and the next message and returns the new accumulator.
            initial_acc: initial accumulator value
            n: maximum number of messages to consume over all topics
            commit_after_processing: commit offsets after processing (independent of auto.offset.commit for the current storage)
            **kwargs: optional kwargs:
                last_n (int): consume only the last n messages
                consume_batch_size (int): consume batch size.
                break_fun (acc, m) -> bool: stop before processing message m given accumulator acc.
                dechunk (bool): dechunk messages

        Returns:
            any: final accumulated value"""
        if n == 0:
            return initial_acc
        #
        (n_int, consume_batch_size_int, consume_timeout_s_float,
         commit_after_processing_bool, break_fun, dechunk_bool) = self._get_consume_options(
            n, commit_after_processing, **kwargs)
        #
        message_counter_int = 0
        #
        acc = initial_acc
        break_bool = False
        #
        idle_s_float = -1
        #
        while True:
            #
            # Consume the next batch of messages.
            (acc, message_counter_int, break_bool,
             has_eligible_messages_bool) = self._consume_batch(
                acc, foldl_fun, message_counter_int, n_int,
                consume_batch_size_int, break_fun, dechunk_bool, **kwargs)
            #
            # If no more messages could be consumed...
            consume_timeout_bool = False
            if not has_eligible_messages_bool:
                # a) ...return if all messages up to the high watermark (at the time of construction of the StorageConsumer instance) are read,
                if all(commit_offset_int >= self.topic_str_partition_int_offsets_tuple_dict_dict[topic_str][partition_int][1] for topic_str, commit_offsets_dict in self.topic_str_commit_offsets_dict_dict.items() for partition_int, commit_offset_int in commit_offsets_dict.items()):
                    break
                # or b) ...return if the consume timeout is reached (counts the idle time between no more messages returned).
                current_time_s_float = time.time()
                if idle_s_float == -1:
                    idle_s_float = current_time_s_float
                else:
                    if current_time_s_float - idle_s_float > consume_timeout_s_float:
                        consume_timeout_bool = True
            else:
                # Reset idle_s_float if messages could be consumed again.
                idle_s_float = -1
            #
            self._commit_offsets(commit_after_processing_bool)
            #
            # Stop consuming for good if break_bool is set (break_fun or message_counter_int >= n_int).
            if break_bool or consume_timeout_bool:
                break
            #
            # Stop consuming for good if the end offset is reached for all topics.
            if all(self.topic_str_commit_offsets_dict_dict[topic_str][partition_int] >= end_offset_int for topic_str, end_offsets_dict in self.topic_str_end_offsets_dict_dict.items() for partition_int, end_offset_int in end_offsets_dict.items()):
                break
        #
        return acc

    #

    def _get_consume_options(self, n_int, commit_after_processing=None, **kwargs):
        """Resolve options shared by foldl() and consume()."""
        # Get last_n (to read n messages from the end of the topic).
        last_n_int = kwargs["last_n"] if "last_n" in kwargs else None
        if last_n_int is not None and last_n_int > 0:
            self.topic_str_assign_offsets_dict_dict = self.get_last_n_start_offsets(last_n_int)
            n_int = last_n_int
        #
        if commit_after_processing is None:
            commit_after_processing_bool = self.storage_obj.commit_after_processing()
        else:
            commit_after_processing_bool = commit_after_processing
        #
        consume_batch_size_int = kwargs["consume_batch_size"] if "consume_batch_size" in kwargs else self.storage_obj.consume_batch_size()
        if n_int != ALL_MESSAGES and consume_batch_size_int > n_int:
            consume_batch_size_int = n_int
        #
        consume_timeout_s_float = kwargs["consume_timeout"] if "consume_timeout" in kwargs else self.storage_obj.consume_timeout()
        break_fun = kwargs["break_fun"] if "break_fun" in kwargs else lambda _, _1: False
        dechunk_bool = kwargs["dechunk"] if "dechunk" in kwargs else False
        #
        return (n_int, consume_batch_size_int, consume_timeout_s_float,
                commit_after_processing_bool, break_fun, dechunk_bool)

    #

    def _consume_batch(self, acc, foldl_fun, message_counter_int, n_int,
                       consume_batch_size_int, break_fun, dechunk_bool, **kwargs):
        """Consume and process exactly one raw batch."""
        m_list1 = self.consume_impl(n=consume_batch_size_int, **kwargs)
        (eligible_m_list, prepared_m_list) = self._prepare_batch(m_list1, dechunk_bool)
        (acc, message_counter_int, break_bool) = self._process_messages(
            prepared_m_list, acc, foldl_fun, message_counter_int, n_int, break_fun)
        #
        return (acc, message_counter_int, break_bool, bool(eligible_m_list))

    #

    def _prepare_batch(self, m_list, dechunk_bool):
        """Filter, dechunk, and deserialize one raw batch of messages.

        Returns:
            tuple: (eligible raw messages, prepared messages)"""
        eligible_m_list = []
        for m in m_list:
            topic_str = m["topic"]
            partition_int = m["partition"]
            offset_int = m["offset"]
            #
            end_offset_int = self.topic_str_end_offsets_dict_dict[topic_str][partition_int]
            # Only collect the messages before the end offset.
            if end_offset_int != 0 and offset_int <= end_offset_int:
                eligible_m_list.append(m)
        #
        if dechunk_bool:
            dechunked_m_list = self.dechunk(eligible_m_list)
        else:
            dechunked_m_list = eligible_m_list
        #
        def deserialize(payload_bytes, type_str, topic_str, key_bool):
            # Do not deserialize if this is a RestProxyConsumer object (deserialization has already taken place on the REST Proxy).
            if self.__class__.__name__ == "RestProxyConsumer":
                return payload_bytes
            else:
                return self.deserialize(payload_bytes, type_str, topic_str, key_bool)
        #
        prepared_m_list = []
        for m in dechunked_m_list:
            topic_str = m["topic"]
            m1 = {"value": deserialize(m["value"], self.topic_str_value_type_str_dict[topic_str], topic_str=topic_str, key_bool=False),
                  "key": deserialize(m["key"], self.topic_str_key_type_str_dict[topic_str], topic_str=topic_str, key_bool=True),
                  "headers": m["headers"],
                  "timestamp": m["timestamp"],
                  "partition": m["partition"],
                  "offset": m["offset"],
                  "topic": topic_str}
            prepared_m_list.append(m1)
        #
        return (eligible_m_list, prepared_m_list)

    #

    def _process_messages(self, m_list, acc, foldl_fun, message_counter_int, n_int, break_fun):
        """Fold over one prepared batch and advance offsets after each processed message."""
        break_bool = False
        for m in m_list:
            if break_fun(acc, m):
                break_bool = True
                break
            #
            acc = foldl_fun(acc, m)
            topic_str = m["topic"]
            partition_int = m["partition"]
            offset_int = m["offset"]
            self.topic_str_commit_offsets_dict_dict[topic_str][partition_int] = offset_int + 1
            message_counter_int += 1
            #
            if n_int != ALL_MESSAGES and message_counter_int >= n_int:
                break_bool = True
                break
        #
        return (acc, message_counter_int, break_bool)

    #

    def _commit_offsets(self, commit_after_processing_bool):
        if self.enable_auto_commit_bool or not commit_after_processing_bool:
            return
        #
        topic_str_valid_offsets_dict_dict = {}
        for topic_str, commit_offsets_dict in self.topic_str_commit_offsets_dict_dict.items():
            valid_offsets_dict = {
                partition_int: offset_int
                for partition_int, offset_int in commit_offsets_dict.items()
                if offset_int != OFFSET_INVALID
            }
            if valid_offsets_dict:
                topic_str_valid_offsets_dict_dict[topic_str] = valid_offsets_dict
        #
        if topic_str_valid_offsets_dict_dict:
            self.commit(topic_str_valid_offsets_dict_dict)

    #

    def consume(self, n=ALL_MESSAGES, **kwargs):
        """Consume one batch of up to n messages as a plain list.

        If n is unset, use the configured consume batch size. Unlike foldl(),
        this method makes only one consume_impl() call.

        Args:
            n: max messages to consume
            **kwargs: passed to consume_impl() or used for batch processing

        Returns:
            list of m: consumed messages"""
        n_int = n
        #
        if n_int == ALL_MESSAGES:
            n_int = self.storage_obj.consume_batch_size()
        #
        if n_int == 0:
            return []
        #
        (n_int, consume_batch_size_int, _, commit_after_processing_bool,
         break_fun, dechunk_bool) = self._get_consume_options(n_int, **kwargs)
        #
        def foldl_fun(m_list, m):
            m_list.append(m)
            #
            return m_list
        #
        consume_kwargs_dict = kwargs.copy()
        consume_kwargs_dict.pop("commit_after_processing", None)
        #
        (m_list1, _, _, _) = self._consume_batch(
            [], foldl_fun, 0, n_int, consume_batch_size_int,
            break_fun, dechunk_bool, **consume_kwargs_dict)
        self._commit_offsets(commit_after_processing_bool)
        #
        return m_list1

    #

    def get_offsets_from_kwargs(self, offsets_key_str, ts_key_str, **kwargs):
        """Resolve per-topic/partition start (or end) offsets from "offsets"/"ts"-style kwargs, replacing negative offsets relative to the watermark.

        Args:
            topic_str_list: topics to resolve offsets for
            offsets_key_str: kwargs key holding explicit offsets, e.g. "offsets"
            ts_key_str: kwargs key holding a timestamp to resolve offsets for, e.g. "ts"
            **kwargs: must contain at most one of offsets_key_str / ts_key_str

        Returns:
            dict: {topic: {partition: offset}}"""
        if offsets_key_str in kwargs and kwargs[offsets_key_str] is not None:
            offsets_dict = kwargs[offsets_key_str]
            first_key_str_or_int = list(offsets_dict.keys())[0]
            if isinstance(first_key_str_or_int, int):
                topic_str_offsets_dict_dict = {topic_str: offsets_dict for topic_str in self.topic_str_list}
            else:
                topic_str_offsets_dict_dict = offsets_dict
        else:
            if ts_key_str in kwargs and kwargs[ts_key_str] is not None:
                timestamp_int = kwargs[ts_key_str]
                topic_str_partitions_int_dict = self.storage_obj.partitions(self.topic_str_list)
                topic_str_partition_int_timestamp_int_dict_dict = {topic_str: {partition_int: timestamp_int for partition_int in range(partitions_int)} for topic_str, partitions_int in topic_str_partitions_int_dict.items()}
                topic_str_offsets_dict_dict = self.storage_obj.offsets_for_times(self.topic_str_list, topic_str_partition_int_timestamp_int_dict_dict, replace_not_found=True)
                if self.storage_obj.verbose() > 0:
                    tqdm.write(f"Offsets for {ts_key_str} ({timestamp_int}):")
                    tqdm.write(topic_str_offsets_dict_dict)
            else:
                topic_str_offsets_dict_dict = {topic_str: {} for topic_str in self.topic_str_list}
        # Check for any negative offsets...
        copied_topic_str_offsets_dict_dict = copy.deepcopy(topic_str_offsets_dict_dict)
        for topic_str, offsets_dict in copied_topic_str_offsets_dict_dict.items():
            if any(offset_int < 0 for offset_int in offsets_dict.values()):
                # If found, replace the negative offset with the current high watermark + the negative offset.
                partition_int_offset_int_tuple_dict = self.topic_str_partition_int_offsets_tuple_dict_dict[topic_str]
                #
                for partition_int, offset_int in offsets_dict.items():
                    if offset_int < 0:
                        topic_str_offsets_dict_dict[topic_str][partition_int] = partition_int_offset_int_tuple_dict[partition_int][1] + offset_int
        #
        return topic_str_offsets_dict_dict

    def get_last_n_start_offsets(self, last_n_int):
        """Compute start offsets that yield exactly the last last_n_int messages across all partitions of each topic.

        Args:
            topic_str_list: topics to compute start offsets for
            last_n_int: total number of trailing messages wanted

        Returns:
            dict: {topic: {partition: start offset}}"""
        topic_str_start_offsets_dict_dict = {}
        #
        for topic_str, partition_int_offset_int_tuple_dict in self.topic_str_partition_int_offsets_tuple_dict_dict.items():
            # state = partition_int_low_watermark_int_high_watermark_int_start_offset_int_tuple_dict - start_offset_int starts with high_watermark_int.
            state = {partition_int: [low_watermark_int, high_watermark_int, high_watermark_int] for partition_int, (low_watermark_int, high_watermark_int) in partition_int_offset_int_tuple_dict.items()}
            for _ in range(last_n_int):
                # Find the partition with the most remaining messages.
                best_partition_int = max(state.keys(), key=lambda partition_int: (state[partition_int][2] - state[partition_int][0], -partition_int))
                if state[best_partition_int][2] == state[best_partition_int][0]:
                    # If not even that partition doesn't carry enough remaining messages, stop.
                    break
                else:
                    # Else "consume" a message from that partition.
                    state[best_partition_int][2] -= 1
            #
            topic_str_start_offsets_dict_dict[topic_str] = {partition_int: start_offset_int for partition_int, (_, _, start_offset_int) in state.items()}
        #
        return topic_str_start_offsets_dict_dict

    def get_key_value_types_from_kwargs(self, **kwargs):
        """Resolve per-topic key/value (de)serialization types from kwargs.

        Args:
            topic_str_list: topics to resolve types for
            **kwargs: passed to storage.get_key_value_type_tuple()

        Returns:
            tuple: ({topic: key_type}, {topic: value_type})"""
        (key_type_str, value_type_str) = self.storage_obj.get_key_value_type_tuple(**kwargs)
        #
        topic_str_key_type_str_dict = {topic_str: key_type_str for topic_str in self.topic_str_list}
        topic_str_value_type_str_dict = {topic_str: value_type_str for topic_str in self.topic_str_list}
        #
        return (topic_str_key_type_str_dict, topic_str_value_type_str_dict)

    def get_partitions_from_kwargs(self, partitions_key_str, **kwargs):
        """Resolve an explicit per-topic partition assignment from kwargs, if given.

        Args:
            partitions_key_str: kwargs key holding the partition assignment, e.g. "partitions"
            **kwargs: optionally containing partitions_key_str

        Returns:
            dict or None: {topic: [partition, ...]}, or None if not given"""
        if partitions_key_str in kwargs and kwargs[partitions_key_str] is not None:
            topic_str_partition_int_list_dict = kwargs[partitions_key_str]
        else:
            topic_str_partition_int_list_dict = None
        #
        return topic_str_partition_int_list_dict

    #

    def get_group_str_from_kwargs(self, **kwargs):
        """Resolve the consumer group name: explicit "group" kwarg, else an auto-generated, prefixed one.

        Args:
            **kwargs: optionally containing group

        Returns:
            str: consumer group name"""
        if "group" in kwargs:
            return kwargs["group"]
        else:
            prefix_str = self.storage_obj.consumer_group_prefix()
            return prefix_str + str(get_millis())
