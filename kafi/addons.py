import time

from kafi.functional import Functional

# Constants

ALL_MESSAGES = -1

#

def default_projection_function(message_dict1, message_dict2):
    message_dict = dict(message_dict1)
    message_dict["value"] = message_dict1["value"] | message_dict2["value"]
    return message_dict
#

class AddOns(Functional):
    def compact(self, topic, n=ALL_MESSAGES, **kwargs):
        def foldl_function(acc, message_dict):
            key_hash_int_message_dict_dict = acc
            #
            key = message_dict["key"]
            value = message_dict["value"]
            #
            if key is not None:
                key_hash_int = hash(str(key))
                if value is None:
                    if key_hash_int in key_hash_int_message_dict_dict:
                        del key_hash_int_message_dict_dict[key_hash_int]
                else:
                    key_hash_int_message_dict_dict[key_hash_int] = message_dict
            #
            return key_hash_int_message_dict_dict
        #

        (key_hash_int_message_dict_dict, _) = self.foldl(topic, foldl_function, {}, n, **kwargs)
        #
        message_dict_list = list(key_hash_int_message_dict_dict.values())
        #
        return message_dict_list

    def compact_to(self, topic, target_storage, target_topic, n=ALL_MESSAGES, **kwargs):
        source_kwargs = self.copy_kwargs("source", **kwargs)
        target_kwargs = self.copy_kwargs("target", **kwargs)
        #
        message_dict_list = self.compact(topic, n, **source_kwargs)
        #
        target_producer = target_storage.producer(target_topic, **target_kwargs)
        key_bytes_list_value_bytes_list_tuple = target_producer.produce_list(message_dict_list, **target_kwargs)
        target_producer.close()
        #
        return key_bytes_list_value_bytes_list_tuple

    #

    def repeat(self, topic_str, n=1, **kwargs):
        n_int = n
        #
        message_dict_list = self.tail(topic_str, type="bytes", n=n_int, **kwargs)
        pr = self.producer(topic_str, type="bytes", **kwargs)
        pr.produce_list(message_dict_list, **kwargs)
        pr.close()
        #
        return message_dict_list

    #

    def recreate(self, topic, partitions=None, config={}, **kwargs):
        topic_str = topic
        #
        if self.exists(topic_str):
            if partitions is None:
                partitions_int = self.partitions(topic_str)[topic_str]
            else:
                partitions_int = partitions
            #
            old_config_dict = self.config(topic_str)[topic_str]
            config_dict = {}
            for key_str, value_str in old_config_dict.items():
                if key_str in config:
                    config_dict[key_str] = config[key_str]
                else:
                    config_dict[key_str] = value_str
            #
            self.delete(topic_str)
            #
            self.create(topic_str, partitions=partitions_int, config=config_dict, **kwargs)
        else:
            if partitions is None:
                partitions_int = 1
            else:
                partitions_int = partitions
            #
            self.create(topic_str, partitions=partitions_int, config=config, **kwargs)
        #
        return topic_str

    retouch = recreate

    #

    def cp_group_offsets(self, pattern, source_group, target_storage, target_group):
        source_group_str = source_group
        target_group_str = target_group
        #
        topic_str_list = self.admin.list_topics(pattern)
        #
        # Get the offsets of the source consumer group.
        source_group_offsets = self.group_offsets(source_group_str)
        #
        # Consume one message from eacg topic with the target consumer group to bring it to life.
        for topic_str in topic_str_list:
            co = target_storage.consumer(topic_str, group=target_group_str, type="bytes")
            co.consume(n=1)
            co.close()
        #
        target_group_offsets = target_storage.group_offsets(target_group, source_group_offsets[source_group_str])
        #
        return target_group_offsets

    #

    def offsets_diff(self, pattern, ts, end_ts, **kwargs):
        ts_int = ts
        end_ts_int = end_ts
        #
        if end_ts_int < ts_int:
            raise Exception(f"End timestamp ({end_ts_int}) before start timestamp ({ts_int}).")
        #
        topic_str_partitions_int_dict = self.partitions(pattern, **kwargs)
        #
        topic_str_messages_int_dict = {}
        for topic_str, partitions_int in topic_str_partitions_int_dict.items():
            start_offsets_dict = self.offsets_for_times(topic_str, {partition_int: ts_int for partition_int in range(partitions_int)}, replace_not_found=True, **kwargs)[topic_str]
            end_offsets_dict = self.offsets_for_times(topic_str, {partition_int: end_ts_int for partition_int in range(partitions_int)}, replace_not_found=True, **kwargs)[topic_str]
            #
            # print(start_offsets_dict)
            # print(end_offsets_dict)
            #
            messages_int = sum([(end_offset_int - start_offset_int) + 1 for start_offset_int, end_offset_int in zip(start_offsets_dict.values(), end_offsets_dict.values())])
            #
            topic_str_messages_int_dict[topic_str] = messages_int
        #
        return topic_str_messages_int_dict

    #

    def message_size(self, topic_str, **kwargs):
        def agg(partition_int_offset_int_size_int_tuple_dict_dict, message_dict):
            partition_int = message_dict["partition"]
            offset_int = message_dict["offset"]
            key_bytes = message_dict["key"]
            key_size_int = 0 if key_bytes is None else len(key_bytes)
            value_bytes = message_dict["value"]
            value_size_int = 0 if value_bytes is None else len(value_bytes)
            #
            if partition_int not in partition_int_offset_int_size_int_tuple_dict_dict:
                partition_int_offset_int_size_int_tuple_dict_dict[partition_int] = {offset_int: None}
            partition_int_offset_int_size_int_tuple_dict_dict[partition_int][offset_int] = (key_size_int, value_size_int)
            return partition_int_offset_int_size_int_tuple_dict_dict
        #
        (partition_int_offset_int_size_int_tuple_dict_dict, n_int) = self.foldl(topic_str, agg, {}, type="bytes", **kwargs)
        #
        return partition_int_offset_int_size_int_tuple_dict_dict, n_int
    
    def message_size_stats(self, topic_str, **kwargs):
        partition_int_offset_int_size_int_tuple_dict_dict, n_int = self.message_size(topic_str, **kwargs)
        #
        total_size_int = 0
        max_dict = {}
        min_dict = {}
        for partition_int, offset_int_size_int_tuple_dict in partition_int_offset_int_size_int_tuple_dict_dict.items():
            for offset_int, (key_size_int, value_size_int) in offset_int_size_int_tuple_dict.items():
                size_int = key_size_int + value_size_int
                #
                total_size_int += size_int
                #
                if max_dict == {}:
                    max_dict = {"size": size_int, "partition": partition_int, "offset": offset_int}
                else:
                    old_max_int = max_dict["size"]
                    new_max_int = max(size_int, old_max_int)
                    if new_max_int != old_max_int:
                        max_dict = {"size": new_max_int, "partition": partition_int, "offset": offset_int}
                #
                if min_dict == {}:
                    min_dict = {"size": size_int, "partition": partition_int, "offset": offset_int}
                else:
                    old_min_int = min_dict["size"]
                    new_min_int = min(size_int, old_min_int)
                    if new_min_int != old_min_int:
                        min_dict = {"size": new_min_int, "partition": partition_int, "offset": offset_int}
                #
        #
        stats_dict = {"messages": n_int, "total_size": total_size_int, "average_size": total_size_int/n_int, "max_size": max_dict, "min_size": min_dict}
        #
        return stats_dict
