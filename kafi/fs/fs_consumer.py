import ast
import os

from kafi.storage_consumer import StorageConsumer

# Constants

ALL_MESSAGES = -1
OFFSET_INVALID = -1001

#

class FSConsumer(StorageConsumer):
    def __init__(self, fs_obj, *topics, **kwargs):
        super().__init__(fs_obj, *topics, **kwargs)
        # Get the list of partitions to consume from the topics.
        if self.topic_str_partition_int_list_dict is None:
            topic_str_partitions_int_dict = self.storage_obj.partitions(self.topic_str_list)
            self.topic_str_partition_int_list_dict = {topic_str: list(range(partitions_int)) for topic_str, partitions_int in topic_str_partitions_int_dict.items()}
        # Get the existing consumer group of the topics if available.
        group_dict = self.storage_obj.admin.get_group_dict(self.group_str)
        if group_dict == {}:
            group_dict["offsets"] = {topic_str: {partition_int: OFFSET_INVALID for partition_int in range(partitions_int)} for topic_str, partitions_int in self.topic_str_partitions_int_dict.items()}
        # Set the consumer group state to stable as we are starting to consume.
        group_dict["state"] = "stable"
        # Persist the new or updated consumer group.
        self.storage_obj.admin.set_group_dict(self.group_str, group_dict)
            
    #

    def close(self):
        new_group_dict = {"state": "empty"}
        self.storage_obj.admin.set_group_dict(self.group_str, new_group_dict)
        #
        return self.topic_str_list

    #
  
    def consume_impl(self, **kwargs):
        n_int = kwargs["n"] if "n" in kwargs and kwargs["n"] != ALL_MESSAGES else 1
        #
        auto_offset_reset_str = self.consumer_config_dict["auto.offset.reset"]
        #
        rel_file_str_list = []
        message_dict_list = []
        message_counter_int = 0
        # Consume the topics sequentially. TODO: Do it in parallel?
        for topic_str in self.topic_str_list:
            partition_int_list = self.topic_str_partition_int_list_dict[topic_str]
            # Get the start offsets.
            start_offsets_dict = {partition_int: offset_int for partition_int, offset_int in self.topic_str_next_offsets_dict_dict[topic_str].items() if partition_int in partition_int_list}
            # If any start offset is not explicitly set, turn to the consumer group.
            if any(start_offsets_dict[partition_int] == OFFSET_INVALID for partition_int in partition_int_list):
                group_dict = self.storage_obj.admin.get_group_dict(self.group_str)["offsets"]
                group_offsets_dict = group_dict.get(topic_str, {})
                #
                for partition_int, offset_int in group_offsets_dict.items():
                    if offset_int != OFFSET_INVALID:
                        start_offsets_dict[partition_int] = offset_int
            # If there is still any offset not explicitly set, turn to auto.offset.reset.
            if any(start_offsets_dict[partition_int] == OFFSET_INVALID for partition_int in partition_int_list):
                # If auto.offset.reset == "latest", get the watermarks to be able to obtain the latest offsets for each partition (do it once here already for all partitions to save Kafka API calls).
                if auto_offset_reset_str.lower() == "latest":
                    partition_int_offset_tuple_dict = self.watermarks(topic_str, **kwargs)[topic_str]
                # Iterate through the offsets of the partitions.
                for partition_int, offset_int in start_offsets_dict.items():
                    # If the partition does not have a committed offset yet, make use of auto.offset.reset.
                    if offset_int == OFFSET_INVALID:
                        if auto_offset_reset_str.lower() == "latest":
                            # ...and if auto.offset.reset == latest, start offset = last offset.
                            start_offsets_dict[partition_int] = partition_int_offset_tuple_dict[partition_int][1]
                        elif auto_offset_reset_str.lower() == "earliest":
                            # ...or if auto.offset.reset == earliest, start offset = 0.
                            start_offsets_dict[partition_int] = 0
                        else:
                            raise Exception("Only \"earliest\" and \"latest\" supported for \"auto.offset.reset\".")
            # Set the next offsets dict for the topic (for further foldl() calls).
            self.topic_str_next_offsets_dict_dict[topic_str] = start_offsets_dict.copy()
            #
            # Get partition files for the partitions to be consumed.
            partition_int_rel_file_str_list_dict = self.storage_obj.admin.get_partition_files(topic_str, [partition_int for partition_int in partition_int_list])
            #
            # Get first partition files for all partitions.
            partition_int_first_partition_rel_file_str_dict = {partition_int: self.storage_obj.admin.find_partition_file_str_by_offset(topic_str, partition_int, offset_int) for partition_int, offset_int in start_offsets_dict.items()}
            #
            # Filter out partitions not corresponding to any file listed by get_partition_files() above.
            partition_int_first_partition_rel_file_str_dict = {partition_int: first_partition_rel_file_str for partition_int, first_partition_rel_file_str in partition_int_first_partition_rel_file_str_dict.items() if first_partition_rel_file_str is not None}
            #
            # Get all partition files to be consumed for all partitions.
            partition_int_to_be_consume_rel_file_str_list_dict = {partition_int: [rel_file_str for rel_file_str in rel_file_str_list if partition_int in partition_int_first_partition_rel_file_str_dict and rel_file_str >= partition_int_first_partition_rel_file_str_dict[partition_int]] for partition_int, rel_file_str_list in partition_int_rel_file_str_list_dict.items()}
            #
            # Create list of partition files to read.
            file_counter_int = 0
            max_num_files_int = max([len(to_be_consume_rel_file_str_list) for to_be_consume_rel_file_str_list in partition_int_to_be_consume_rel_file_str_list_dict.values()])
            #
            for file_counter_int in range(max_num_files_int):
                for partition_int in partition_int_list:
                    if partition_int in partition_int_to_be_consume_rel_file_str_list_dict:
                        if len(partition_int_to_be_consume_rel_file_str_list_dict[partition_int]) > file_counter_int:
                            rel_file_str_list.append(partition_int_to_be_consume_rel_file_str_list_dict[partition_int][file_counter_int])
            #
            abs_topic_dir_str = self.storage_obj.admin.get_topic_abs_path_str(topic_str)
            break_bool = False
            for rel_file_str in rel_file_str_list:
                if break_bool:
                    break
                #
                messages_bytes = self.storage_obj.admin.read_bytes(os.path.join(abs_topic_dir_str, "partitions", rel_file_str))
                #
                message_bytes_list = messages_bytes.split(b"\n")[:-1]
                #
                for message_bytes in message_bytes_list:
                    message_dict = ast.literal_eval(message_bytes.decode("utf-8"))
                    #
                    partition_int = message_dict["partition"]
                    offset_int = message_dict["offset"]
                    self.topic_str_next_offsets_dict_dict[topic_str][partition_int] = offset_int + 1
                    if self.enable_auto_commit_bool:
                        # Commit immediately after reading the message if enable.auto.commit == True
                        self.commit()
                    #
                    if self.topic_str_end_offsets_dict_dict is not None and topic_str in self.topic_str_end_offsets_dict_dict:
                        end_offsets_dict = self.topic_str_end_offsets_dict_dict[topic_str]
                        if offset_int > end_offsets_dict[partition_int]:
                            break_bool = True
                            break
                    #
                    if offset_int >= start_offsets_dict[partition_int]:
                        message_dict_list.append(message_dict)
                        #
                        message_counter_int += 1
                    #
                    if self.topic_str_end_offsets_dict_dict is not None and topic_str in self.topic_str_end_offsets_dict_dict:
                        end_offsets_dict = self.topic_str_end_offsets_dict_dict[topic_str]
                        offsets_dict = self.topic_str_next_offsets_dict_dict[topic_str]
                        if all(offsets_dict[partition_int] > end_offset_int for partition_int, end_offset_int in end_offsets_dict.items() if partition_int in offsets_dict):
                            break_bool = True
                            break
                    #
                    if n_int != ALL_MESSAGES and message_counter_int >= n_int:
                        break_bool = True
                        break
        #
        return message_dict_list

    #

    def offsets(self):
        group_str_topic_str_offsets_dict_dict_dict = self.storage_obj.admin.group_offsets(self.group_str)
        #
        if self.group_str not in group_str_topic_str_offsets_dict_dict_dict:
            topic_str_offsets_dict_dict = {topic_str: {} for topic_str in self.topic_str_list}
        else:
            topic_str_offsets_dict_dict = {topic_str: offsets_dict for topic_str, offsets_dict in group_str_topic_str_offsets_dict_dict_dict[self.group_str].items() if topic_str in self.topic_str_list}
        #
        return topic_str_offsets_dict_dict

    #

    def commit(self, offsets=None, **kwargs):
        if offsets is None:
            new_group_dict = {"offsets": self.topic_str_next_offsets_dict_dict}
            #
            topic_str_offsets_dict_dict = {topic_str: self.topic_str_next_offsets_dict_dict[topic_str] for topic_str in self.topic_str_list}
        else:
            str_or_int = list(offsets.keys())[0]
            if isinstance(str_or_int, str):
                topic_str_offsets_dict_dict = offsets
            elif isinstance(str_or_int, int):
                topic_str_offsets_dict_dict = {topic_str: offsets for topic_str in self.topic_str_list}
            #
            new_group_dict = {"offsets": topic_str_offsets_dict_dict}
        #
        self.storage_obj.admin.set_group_dict(self.group_str, new_group_dict)
        #
        return topic_str_offsets_dict_dict
