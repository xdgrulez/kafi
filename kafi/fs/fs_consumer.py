import ast
import json
import os

from kafi.storage_consumer import StorageConsumer

# Constants

ALL_MESSAGES = -1
OFFSET_INVALID = -1001

#

class FSConsumer(StorageConsumer):
    def __init__(self, fs_obj, *topics, **kwargs):
        super().__init__(fs_obj, *topics, **kwargs)
        #
        self.next_topic_str_offsets_dict_dict = {topic_str: {partition_int: OFFSET_INVALID for partition_int in range(self.storage_obj.admin.get_partitions(topic_str))} for topic_str in self.topic_str_list}
        new_group_dict = {"offsets": self.next_topic_str_offsets_dict_dict, "state": "stable"}
        self.storage_obj.admin.set_group_dict(self.group_str, new_group_dict)

    #

    def close(self):
        new_group_dict = {"state": "empty"}
        self.storage_obj.admin.set_group_dict(self.group_str, new_group_dict)
    
    #
  
    def foldl(self, foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        auto_offset_reset_str = self.consumer_config_dict["auto.offset.reset"]
        #
        message_counter_int = 0
        acc = initial_acc
        for topic_str in self.topic_str_list:
            partitions_int = self.storage_obj.admin.get_partitions(topic_str)
            #
            abs_topic_dir_str = self.storage_obj.admin.get_topic_abs_dir_str(topic_str)
            # Get start offsets.
            start_offsets_dict = self.topic_str_start_offsets_dict_dict[topic_str] if self.topic_str_start_offsets_dict_dict is not None and topic_str in self.topic_str_start_offsets_dict_dict else None
            #
            if start_offsets_dict is None:
                # If we got no start offsets, try to get the start offsets from the group.
                group_dict = self.storage_obj.admin.get_group_dict(self.group_str)
                group_offsets_dict = group_dict["offsets"][topic_str]
                #
                if any(group_offsets_dict[partition_int] == OFFSET_INVALID for partition_int in range(partitions_int)):
                    # If any of the partitions still does not have a committed offset yet...
                    if auto_offset_reset_str.lower() == "latest":
                        # If auto.offset.reset == latest, start offsets = last offsets.
                        partition_int_offset_tuple_dict = self.watermarks(topic_str, **kwargs)[topic_str]
                        auto_offset_reset_offsets_dict = {partition_int: offset_tuple[1] for partition_int, offset_tuple in partition_int_offset_tuple_dict.items()}
                    elif auto_offset_reset_str.lower() == "earliest":
                        # If auto.offset.reset == earliest, start offsets = 0
                        auto_offset_reset_offsets_dict = {partition_int: 0 for partition_int in range(partitions_int)}
                    else:
                        raise Exception("Only \"earliest\" and \"latest\" supported for \"auto.offset.reset\".")
                    # ...and for each partition still not having a committed offset yet, use the auto.offset.reset one.
                    start_offsets_dict = {}
                    for partition_int, offset_int in group_offsets_dict.items():
                        if offset_int == OFFSET_INVALID:
                            start_offsets_dict[partition_int] = auto_offset_reset_offsets_dict[partition_int]
                        else:
                            start_offsets_dict[partition_int] = group_offsets_dict[partition_int]
                else:
                    start_offsets_dict = group_offsets_dict
            #
            message_separator_bytes = self.storage_obj.admin.get_message_separator(topic_str)
            #
            # Get partition files for all partitions.
            partition_int_rel_file_str_list_dict = self.storage_obj.admin.get_partition_files(topic_str)
            #
            # Get first partition files for all partitions.
            partition_int_first_partition_rel_file_str_dict = {partition_int: self.storage_obj.admin.find_partition_file_str(topic_str, partition_int, offset_int) for partition_int, offset_int in start_offsets_dict.items()}
            #
            # Filter out partitions not corresponding to any filee listed by get_partition_files() above.
            partition_int_first_partition_rel_file_str_dict = {partition_int: first_partition_rel_file_str for partition_int, first_partition_rel_file_str in partition_int_first_partition_rel_file_str_dict.items() if first_partition_rel_file_str is not None}
            #
            # Get all partition files to be consumed for all partitions.
            partition_int_to_be_consume_rel_file_str_list_dict = {partition_int: [rel_file_str for rel_file_str in rel_file_str_list if partition_int in partition_int_first_partition_rel_file_str_dict and rel_file_str >= partition_int_first_partition_rel_file_str_dict[partition_int]] for partition_int, rel_file_str_list in partition_int_rel_file_str_list_dict.items()}
            #
            def acc_bytes_to_acc(acc, message_bytes, message_counter_int):
                serialized_message_dict = ast.literal_eval(message_bytes.decode("utf-8"))
                #
                self.next_topic_str_offsets_dict_dict[topic_str][serialized_message_dict["partition"]] = serialized_message_dict["offset"] + 1
                if self.enable_auto_commit_bool:
                    # Commit immediately after reading the message if enable.auto.commit == True
                    self.commit()
                #
                if serialized_message_dict["offset"] >= start_offsets_dict[partition_int]:
                    deserialized_message_dict = deserialize(serialized_message_dict, self.topic_str_key_type_str_dict[topic_str], self.topic_str_value_type_str_dict[topic_str])
                    #
                    acc = foldl_function(acc, deserialized_message_dict)
                    #
                    message_counter_int += 1
                    #
                    if not self.enable_auto_commit_bool and self.storage_obj.commit_after_processing():
                        # Only commit once the message has been processed if enable.auto.commit == False and commit.after.processing == True
                        self.commit()
                #
                return (acc, message_counter_int)
                #

        def deserialize(message_dict, key_type, value_type):
            key_type_str = key_type
            value_type_str = value_type
            #

            def to_str(x):
                if isinstance(x, bytes):
                    return x.decode("utf-8")
                elif isinstance(x, dict):
                    return str(x)
                else:
                    return x
            #

            def to_bytes(x):
                if isinstance(x, str):
                    return x.encode("utf-8")
                elif isinstance(x, dict):
                    return str(x).encode("utf-8")
                else:
                    return x
            #

            def to_dict(x):
                if isinstance(x, bytes) or isinstance(x, str):
                    return json.loads(x)
                else:
                    return x
            #

            if key_type_str.lower() == "str":
                decode_key = to_str
            elif key_type_str.lower() == "bytes":
                decode_key = to_bytes
            elif key_type_str.lower() == "json":
                decode_key = to_dict
            else:
                raise Exception("Only json, str or bytes supported.")
            #
            if value_type_str.lower() == "str":
                decode_value = to_str
            elif value_type_str.lower() == "bytes":
                decode_value = to_bytes
            elif value_type_str.lower() == "json":
                decode_value = to_dict
            else:
                raise Exception("Only json, str or bytes supported.")
            #
            return_message_dict = {"headers": message_dict["headers"], "timestamp": message_dict["timestamp"], "key": decode_key(message_dict["key"]), "value": decode_value(message_dict["value"]), "offset": message_dict["offset"], "partition": message_dict["partition"]}
            return return_message_dict
        
        #

        file_counter_int = 0
        max_num_files_int = max([len(to_be_consume_rel_file_str_list) for to_be_consume_rel_file_str_list in partition_int_to_be_consume_rel_file_str_list_dict.values()])
        rel_file_str_list = []
        #
        for file_counter_int in range(max_num_files_int):
            for partition_int in range(partitions_int):
                if partition_int in partition_int_to_be_consume_rel_file_str_list_dict:
                    if len(partition_int_to_be_consume_rel_file_str_list_dict[partition_int]) > file_counter_int:
                        rel_file_str_list.append(partition_int_to_be_consume_rel_file_str_list_dict[partition_int][file_counter_int])
        #
        for rel_file_str in rel_file_str_list:
            message_bytes_list = self.storage_obj.admin.read_messages_from_file(os.path.join(abs_topic_dir_str, "partitions", rel_file_str), message_separator_bytes)
            for message_bytes in message_bytes_list:
                (acc, message_counter_int) = acc_bytes_to_acc(acc, message_bytes, message_counter_int)
                #
                if n_int != ALL_MESSAGES:
                    if message_counter_int >= n_int:
                        return acc
        #
        return acc

    #

    def consume(self, n=ALL_MESSAGES):
        def foldl_function(message_dict_list, message_dict):
            message_dict_list.append(message_dict)
            #
            return message_dict_list
        #
        return self.foldl(foldl_function, [], n)

    def offsets(self):
        group_str_topic_str_offsets_dict_dict_dict = self.storage_obj.admin.group_offsets(self.group_str)
        #
        if self.group_str not in group_str_topic_str_offsets_dict_dict_dict:
            topic_str_offsets_dict_dict = {topic_str: {} for topic_str in self.topic_str_list}
        else:
            topic_str_offsets_dict_dict = {topic_str: offsets_dict for topic_str, offsets_dict in group_str_topic_str_offsets_dict_dict_dict[self.group_str].items() if topic_str in self.topic_str_list}
        #
        return topic_str_offsets_dict_dict

    def commit(self, offsets=None):
        if offsets is None:
            new_group_dict = {"offsets": self.next_topic_str_offsets_dict_dict}
            #
            topic_str_group_str_offsets_dict_dict_dict = {topic_str: {self.group_str: self.next_topic_str_offsets_dict_dict[topic_str]} for topic_str in self.topic_str_list}
        else:
            topic_str_group_str_offsets_dict_dict_dict = offsets
            #
            new_group_dict = {"offsets": {topic_str: group_str_offsets_dict_dict[self.group_str] for topic_str, group_str_offsets_dict_dict in topic_str_group_str_offsets_dict_dict_dict.items()}}
        #
        self.storage_obj.admin.set_group_dict(self.group_str, new_group_dict)
        #
        return topic_str_group_str_offsets_dict_dict_dict
