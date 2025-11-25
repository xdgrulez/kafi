from kafi.dechunker import Dechunker
from kafi.helpers import get_millis

# Constants

ALL_MESSAGES = -1

#

class StorageConsumer(Dechunker):
    def __init__(self, storage_obj, *topics, **kwargs):
        self.storage_obj = storage_obj
        #
        super().__init__(storage_obj.schema_registry_config_dict, **kwargs)
        #
        # Get topics to subscribe to.
        self.topic_str_list = list(topics)
        #
        # Get start offsets for the subscribed topics.
        self.topic_str_start_offsets_dict_dict = self.get_offsets_from_kwargs(self.topic_str_list, "offsets", "ts", **kwargs)
        self.topic_str_end_offsets_dict_dict = self.get_offsets_from_kwargs(self.topic_str_list, "end_offsets", "end_ts", **kwargs)
        #
        # Get partitions to assign for the subscribed topics.
        self.topic_str_partition_int_list_dict = self.get_partitions_from_kwargs("partitions", **kwargs)
        # Get key and value types.
        (self.topic_str_key_type_str_dict, self.topic_str_value_type_str_dict) = self.get_key_value_types_from_kwargs(self.topic_str_list, **kwargs)
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
        self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict = {}

    #

    def foldl(self, foldl_function, initial_acc, n=ALL_MESSAGES, commit_after_processing=None, **kwargs):
        n_int = n
        #
        if n_int == 0:
            return initial_acc
        #
        commit_after_processing_bool = self.storage_obj.commit_after_processing() if commit_after_processing is None else commit_after_processing
        #
        consume_batch_size_int = kwargs["consume_batch_size"] if "consume_batch_size" in kwargs else self.storage_obj.consume_batch_size()
        if n != ALL_MESSAGES and consume_batch_size_int > n_int:
            consume_batch_size_int = n_int
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        topic_str_partitions_int_dict = self.storage_obj.partitions(self.topic_str_list)
        topic_str_offsets_dict_dict = {topic_str: {partition_int: 0 for partition_int in range(partitions_int)} for topic_str, partitions_int in topic_str_partitions_int_dict.items()}
        #
        message_counter_int = 0
        #
        acc = initial_acc
        break_bool = False
        #
        def deserialize(payload_bytes, type_str, topic_str, key_bool):
            # Do not deserialize if this is a RestProxyConsumer object (deserialization has already taken place on the REST Proxy). 
            if self.__class__.__name__ == "RestProxyConsumer":
                return payload_bytes
            else:
                return self.deserialize(payload_bytes, type_str, topic_str, key_bool)
        #
        while True:
            message_dict_list1 = self.consume_impl(n=consume_batch_size_int, **kwargs)
            if not message_dict_list1:
                break
            #
            # Dechunk if necessary.
            message_dict_list2 = self.dechunk(message_dict_list1)
            #
            for message_dict in message_dict_list2:
                topic_str = message_dict["topic"]
                partition_int = message_dict["partition"]
                offset_int = message_dict["offset"]
                #
                offsets_dict = topic_str_offsets_dict_dict[topic_str]
                #
                offsets_dict[partition_int] = offset_int + 1
                #
                if self.topic_str_end_offsets_dict_dict is not None and topic_str in self.topic_str_end_offsets_dict_dict:
                    end_offsets_dict = self.topic_str_end_offsets_dict_dict[topic_str]
                    if offset_int > end_offsets_dict[partition_int]:
                        continue
                #
                message_dict1 = {"value": deserialize(message_dict["value"], self.topic_str_value_type_str_dict[topic_str], topic_str=topic_str, key_bool=False),
                                 "key": deserialize(message_dict["key"], self.topic_str_key_type_str_dict[topic_str], topic_str=topic_str, key_bool=True),
                                 "headers": message_dict["headers"],
                                 "timestamp": message_dict["timestamp"],
                                 "partition": message_dict["partition"],
                                 "offset": message_dict["offset"],
                                 "topic": message_dict["topic"]}
                #
                if break_function(acc, message_dict1):
                    break_bool = True
                    break
                #
                acc = foldl_function(acc, message_dict1)
                message_counter_int += 1
                #
                if self.topic_str_end_offsets_dict_dict is not None and topic_str in self.topic_str_end_offsets_dict_dict:
                    end_offsets_dict = self.topic_str_end_offsets_dict_dict[topic_str]
                    if all(offsets_dict[partition_int] > end_offset_int for partition_int, end_offset_int in end_offsets_dict.items()):
                        break_bool = True
                        break
                #
                if n_int != ALL_MESSAGES and message_counter_int >= n_int:
                    break_bool = True
                    break
            #
            if not self.enable_auto_commit_bool and commit_after_processing_bool:
                self.commit(topic_str_offsets_dict_dict)
            #
            if break_bool:
                break
        #
        return acc

    #

    def consume(self, n=ALL_MESSAGES, **kwargs):
        def foldl_function(message_dict_list, message_dict):
            message_dict_list.append(message_dict)
            #
            return message_dict_list
        #
        return self.foldl(foldl_function, [], n)
        # return self.foldl(foldl_function, [], n, commit_after_processing=False, **kwargs) from kafka_consumer???

    #

    def get_offsets_from_kwargs(self, topic_str_list, offsets_key_str, ts_key_str, **kwargs):
        if offsets_key_str in kwargs and kwargs[offsets_key_str] is not None:
            offsets_dict = kwargs[offsets_key_str]
            first_key_str_or_int = list(offsets_dict.keys())[0]
            if isinstance(first_key_str_or_int, int):
                topic_str_offsets_dict_dict = {topic_str: offsets_dict for topic_str in topic_str_list}
            else:
                topic_str_offsets_dict_dict = offsets_dict
        else:
            if ts_key_str in kwargs and kwargs[ts_key_str] is not None:
                timestamp_int = kwargs[ts_key_str]
                topic_str_partitions_int_dict = self.storage_obj.partitions(topic_str_list)
                topic_str_partition_int_timestamp_int_dict_dict = {topic_str: {partition_int: timestamp_int for partition_int in range(partitions_int)} for topic_str, partitions_int in topic_str_partitions_int_dict.items()}
                topic_str_offsets_dict_dict = self.storage_obj.offsets_for_times(topic_str_list, topic_str_partition_int_timestamp_int_dict_dict, replace_not_found=True)
                if self.storage_obj.verbose() > 0:
                    print(f"Offsets for {ts_key_str} ({timestamp_int}):")
                    print(topic_str_offsets_dict_dict)
            else:
                topic_str_offsets_dict_dict = None
        # Check for any negative offsets - in that case, use current high watermark + negative offset as the offset.
        # print(topic_str_offsets_dict_dict)
        if topic_str_offsets_dict_dict is not None:
            offset_int_list = sum({topic_str: list(offsets_dict.values()) for topic_str, offsets_dict in topic_str_offsets_dict_dict.items()}.values(), [])
            if any(offset_int < 0 for offset_int in offset_int_list):
                topic_str_partition_int_offset_int_tuple_dict_dict = self.storage_obj.watermarks(topic_str_list)
                topic_str_offsets_dict_dict = {topic_str: {partition_int: (topic_str_partition_int_offset_int_tuple_dict_dict[topic_str][partition_int][1] + offset_int if topic_str_partition_int_offset_int_tuple_dict_dict[topic_str][partition_int][1] > 0 else 0) for partition_int, offset_int in offsets_dict.items() if offset_int < 0} for topic_str, offsets_dict in topic_str_offsets_dict_dict.items()}
        #
        # print(topic_str_offsets_dict_dict)
        #
        return topic_str_offsets_dict_dict

    def get_key_value_types_from_kwargs(self, topic_str_list, **kwargs):
        (key_type_str, value_type_str) = self.storage_obj.get_key_value_type_tuple(**kwargs)
        #
        topic_str_key_type_str_dict = {topic_str: key_type_str for topic_str in topic_str_list}
        topic_str_value_type_str_dict = {topic_str: value_type_str for topic_str in topic_str_list}
        #
        return (topic_str_key_type_str_dict, topic_str_value_type_str_dict)

    def get_partitions_from_kwargs(self, partitions_key_str, **kwargs):
        if partitions_key_str in kwargs and kwargs[partitions_key_str] is not None:
            topic_str_partition_int_list_dict = kwargs[partitions_key_str]
        else:
            topic_str_partition_int_list_dict = None
        #
        return topic_str_partition_int_list_dict

    #

    def get_group_str_from_kwargs(self, **kwargs):
        if "group" in kwargs:
            return kwargs["group"]
        else:
            prefix_str = self.storage_obj.consumer_group_prefix()
            return prefix_str + str(get_millis())
