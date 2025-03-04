from kafi.helpers import pattern_match

#

class StorageAdmin():
    def __init__(self, storage_obj, **kwargs):
        self.storage_obj = storage_obj

    #

    def topics(self, pattern=None, size=False, **kwargs):
        pattern_str_or_str_list = pattern
        size_bool = size
        partitions_bool = "partitions" in kwargs and kwargs["partitions"]
        #
        timeout_int = kwargs["timeout"] if "timeout" in kwargs else -1.0
        #
        def size(pattern_str_or_str_list, timeout_int):
            topic_str_partition_int_tuple_dict_dict = self.watermarks(pattern_str_or_str_list, timeout=timeout_int)
            #
            topic_str_size_int_partitions_dict_tuple_dict = {}
            for topic_str, partition_int_tuple_dict in topic_str_partition_int_tuple_dict_dict.items():
                partitions_dict = {partition_int: partition_int_tuple_dict[partition_int][1]-partition_int_tuple_dict[partition_int][0] for partition_int in partition_int_tuple_dict.keys()}
                #
                size_int = 0
                for offset_int_tuple in partition_int_tuple_dict.values():
                    partition_size_int = offset_int_tuple[1] - offset_int_tuple[0]
                    size_int += partition_size_int
                #
                topic_str_size_int_partitions_dict_tuple_dict[topic_str] = (size_int, partitions_dict)
            return topic_str_size_int_partitions_dict_tuple_dict
        #

        if size_bool:
            topic_str_size_int_partitions_dict_tuple_dict = size(pattern_str_or_str_list, timeout_int)
            if partitions_bool:
                # e.g. {"topic": {"size": 42, "partitions": {0: 23, 1: 4711}}}
                topic_str_size_int_partitions_dict_dict = {topic_str: {"size": size_int_partitions_dict_tuple[0], "partitions": size_int_partitions_dict_tuple[1]} for topic_str, size_int_partitions_dict_tuple in topic_str_size_int_partitions_dict_tuple_dict.items()}
                return topic_str_size_int_partitions_dict_dict
            else:
                # e.g. {"topic": 42}
                topic_str_size_int_dict = {topic_str: size_int_partitions_dict_tuple[0] for topic_str, size_int_partitions_dict_tuple in topic_str_size_int_partitions_dict_tuple_dict.items()}
                return topic_str_size_int_dict
        else:
            if partitions_bool:
                # e.g. {"topic": {0: 23, 1: 4711}}
                topic_str_size_int_partitions_dict_tuple_dict = size(pattern_str_or_str_list, timeout_int)
                topic_str_partitions_dict_dict = {topic_str: size_int_partitions_dict_tuple[1] for topic_str, size_int_partitions_dict_tuple in topic_str_size_int_partitions_dict_tuple_dict.items()}
                return topic_str_partitions_dict_dict
            else:
                # e.g. ["topic"]
                topic_str_list = self.list_topics(pattern_str_or_str_list)
                filtered_topic_str_list = pattern_match(topic_str_list, pattern_str_or_str_list)
                return filtered_topic_str_list

    #

    # partitions_timestamps can either be:
    # * a dictionary mapping partitions to timestamps (if the first key is an integer)
    # * a dictionary mapping topics to a dictionary mapping partitions to timestamps (if the first key is a string for multiple individual topics)
    # => consolidate to the latter.
    def get_topic_str_partition_int_timestamp_int_dict_dict(self, topic_str_list, partitions_timestamps):
        first_key_str_or_int = list(partitions_timestamps.keys())[0]
        if isinstance(first_key_str_or_int, int):
            partition_int_timestamp_int_dict = partitions_timestamps
            topic_str_partition_int_timestamp_int_dict_dict = {topic_str: partition_int_timestamp_int_dict for topic_str in topic_str_list}
        else:
            topic_str_partition_int_timestamp_int_dict_dict = partitions_timestamps
        #
        return topic_str_partition_int_timestamp_int_dict_dict 

    def replace_not_found(self, topic_str_offsets_dict_dict):
        for topic_str, offsets_dict in topic_str_offsets_dict_dict.items():
            if any(offset_int == -1 for offset_int in offsets_dict.values()):
                partition_int_offsets_tuple_dict = self.storage_obj.watermarks(topic_str)[topic_str]
                for partition_int, offset_int in offsets_dict.items():
                    if offset_int == -1:
                        high_watermark_int = partition_int_offsets_tuple_dict[partition_int][1]
                        offsets_dict[partition_int] = high_watermark_int - 1 if high_watermark_int >= 0 else 0
                #
                topic_str_offsets_dict_dict[topic_str] = offsets_dict
        #
        return topic_str_offsets_dict_dict
