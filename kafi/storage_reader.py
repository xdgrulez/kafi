class StorageReader():
    def __init__(self, storage_obj, *topics, **kwargs):
        self.storage_obj = storage_obj
        #
        self.topic_str_list = list(topics)
        #
        self.topic_str_offsets_dict_dict = self.get_topic_str_offsets_dict_dict(self.topic_str_list, **kwargs)
        #
        (self.key_type_str, self.value_type_str) = self.storage_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.key_type_dict, self.value_type_dict) = self.get_key_value_type_dict_tuple(self.key_type_str, self.value_type_str, self.topic_str_list)
    
    #

    def get_topic_str_offsets_dict_dict(self, topic_str_list, **kwargs):
        if "offsets" in kwargs and kwargs["offsets"] is not None:
            offsets_dict = kwargs["offsets"]
            str_or_int = list(offsets_dict.keys())[0]
            if isinstance(str_or_int, int):
                topic_str_offsets_dict_dict = {topic_str: offsets_dict for topic_str in topic_str_list}
            else:
                topic_str_offsets_dict_dict = offsets_dict
        else:
            topic_str_offsets_dict_dict = None
        #
        if topic_str_offsets_dict_dict is not None:
            offset_int_list = sum({topic_str: list(offsets_dict.values()) for topic_str, offsets_dict in topic_str_offsets_dict_dict.items()}.values(), [])
            if any(offset_int < 0 for offset_int in offset_int_list):
                topic_str_partition_int_size_int_dict_dict = self.storage_obj.ls(topic_str_list, partitions=True)
                topic_str_offsets_dict_dict = {topic_str: {partition_int: (topic_str_partition_int_size_int_dict_dict[topic_str][partition_int] + offset_int if offset_int < 0 else offset_int) for partition_int, offset_int in offsets_dict.items()} for topic_str, offsets_dict in topic_str_offsets_dict_dict.items()}
        #
        return topic_str_offsets_dict_dict

    def get_key_value_type_dict_tuple(self, key_type, value_type, topic_str_list):
        if isinstance(key_type, dict):
            key_type_dict = key_type
        else:
            key_type_dict = {topic_str: key_type for topic_str in topic_str_list}
        if isinstance(value_type, dict):
            value_type_dict = value_type
        else:
            value_type_dict = {topic_str: value_type for topic_str in topic_str_list}
        #
        return (key_type_dict, value_type_dict)
