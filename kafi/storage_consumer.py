from kafi.deserializer import Deserializer
from kafi.helpers import get_millis, to_millis

class StorageConsumer(Deserializer):
    def __init__(self, storage_obj, *topics, **kwargs):
        self.storage_obj = storage_obj
        #
        super().__init__(storage_obj.schema_registry_config_dict)
        #
        # Get topics to subscribe to.
        self.topic_str_list = list(topics)
        #
        # Get start offsets for the subscribed topics.
        self.topic_str_start_offsets_dict_dict = self.get_offsets_from_kwargs(self.topic_str_list, "offsets", "ts", **kwargs)
        self.topic_str_end_offsets_dict_dict = self.get_offsets_from_kwargs(self.topic_str_list, "end_offsets", "end_ts", **kwargs)
        #
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
                topic_str_offsets_dict_dict = self.storage_obj.offsets_for_times(topic_str_list, topic_str_partition_int_timestamp_int_dict_dict)
            else:
                topic_str_offsets_dict_dict = None
        # Check for any negative offsets - in that case, use current high watermark + negative offset as the offset.
        if topic_str_offsets_dict_dict is not None:
            offset_int_list = sum({topic_str: list(offsets_dict.values()) for topic_str, offsets_dict in topic_str_offsets_dict_dict.items()}.values(), [])
            if any(offset_int < 0 for offset_int in offset_int_list):
                topic_str_partition_int_offset_int_tuple_dict_dict = self.storage_obj.watermarks(topic_str_list)
                topic_str_offsets_dict_dict = {topic_str: {partition_int: (topic_str_partition_int_offset_int_tuple_dict_dict[topic_str][partition_int][1] + offset_int if offset_int < 0 else offset_int) for partition_int, offset_int in offsets_dict.items()} for topic_str, offsets_dict in topic_str_offsets_dict_dict.items()}
        #
        return topic_str_offsets_dict_dict

    def get_key_value_types_from_kwargs(self, topic_str_list, **kwargs):
        (key_type_str, value_type_str) = self.storage_obj.get_key_value_type_tuple(**kwargs)
        #
        topic_str_key_type_str_dict = {topic_str: key_type_str for topic_str in topic_str_list}
        topic_str_value_type_str_dict = {topic_str: value_type_str for topic_str in topic_str_list}
        #
        return (topic_str_key_type_str_dict, topic_str_value_type_str_dict)

    #

    def get_group_str_from_kwargs(self, **kwargs):
        if "group" in kwargs:
            return kwargs["group"]
        else:
            prefix_str = self.storage_obj.consumer_group_prefix()
            return prefix_str + str(get_millis())
