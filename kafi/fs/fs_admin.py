import ast
from fnmatch import fnmatch
import os

from kafi.storage_admin import StorageAdmin
from kafi.helpers import get_millis, pattern_match

class FSAdmin(StorageAdmin):
    def __init__(self, fs_obj, **kwargs):
        super().__init__(fs_obj, **kwargs)
        #
        self.default_state_str = "stable"

    #

    def list_topics(self, pattern=None):
        root_dir_str = self.storage_obj.root_dir()
        rel_dir_str_list = self.list_dirs(os.path.join(root_dir_str, "topics"))
        #
        all_topic_str_list = [rel_dir_str for rel_dir_str in rel_dir_str_list if not rel_dir_str.endswith("partitions")]
        #
        topic_or_file_str_list = pattern_match(all_topic_str_list, pattern)
        #
        return topic_or_file_str_list

    def list_groups(self, pattern=None):
        root_dir_str = self.storage_obj.root_dir()
        rel_file_str_list = self.list_files(os.path.join(root_dir_str, "groups"))
        #
        all_group_str_list = [rel_file_str for rel_file_str in rel_file_str_list]
        all_group_str_set = set(all_group_str_list)
        all_group_str_list = list(all_group_str_set)
        #
        group_str_list = pattern_match(all_group_str_list, pattern)
        #
        return group_str_list

    #

    def config(self, pattern, config=None, **kwargs):
        config_dict = config
        #
        topic_str_list = self.list_topics(pattern)
        #
        if config_dict is not None:
            for topic_str in topic_str_list:
                metadata_dict = self.get_metadata(topic_str)
                metadata_dict["config"] = config_dict
                self.set_metadata(topic_str, metadata_dict)
        #
        topic_str_config_dict_dict = {topic_str: self.get_config(topic_str) for topic_str in topic_str_list}
        #
        return topic_str_config_dict_dict

    #

    def create(self, topic, partitions=1, config={}, **kwargs):
        topic_str = topic
        #
        if self.list_topics(topic_str) != []:
            raise Exception(f"Topic \"{topic_str}\" already exists.")
        #
        config_dict = config
        partitions_int = partitions
        #
        metadata_dict = {"topic": topic_str, "partitions": partitions_int, "config": config_dict}
        self.set_metadata(topic_str, metadata_dict)
    
    #

    def delete(self, pattern, **kwargs):
        topic_str_list = self.list_topics(pattern)
        #
        for topic_str in topic_str_list:
            topic_abs_dir_str = self.get_topic_abs_path_str(topic_str)
            #
            rel_file_str_list = self.list_files(topic_abs_dir_str)
            for rel_file_str in rel_file_str_list:
                self.delete_file(os.path.join(topic_abs_dir_str, rel_file_str))
            #
            rel_dir_str_list = self.list_dirs(topic_abs_dir_str)
            for rel_dir_str in rel_dir_str_list:
                self.delete_dir(os.path.join(topic_abs_dir_str, rel_dir_str))
            #
            self.delete_dir(topic_abs_dir_str)
        #
        return topic_str_list
    
    #

    def offsets_for_times(self, pattern, partitions_timestamps, replace_not_found=False, **kwargs):
        pattern_str_or_str_list = pattern
        replace_not_found_bool = replace_not_found
        #
        topic_str_list = self.list_topics(pattern_str_or_str_list)
        #
        topic_str_partition_int_timestamp_int_dict_dict = self.get_topic_str_partition_int_timestamp_int_dict_dict(topic_str_list, partitions_timestamps)
        #
        topic_str_offsets_dict_dict = {}
        for topic_str in topic_str_list:
            offsets_dict = {}
            #
            partitions_int = self.get_partitions(topic_str)
            #
            topic_str_offsets_dict_dict[topic_str] = {partition_int: -1 for partition_int in range(partitions_int)}
            offsets_dict = topic_str_offsets_dict_dict[topic_str]
            #
            abs_topic_dir_str = self.get_topic_abs_path_str(topic_str)
            #
            for partition_int in range(partitions_int):
                rel_file_str = self.find_partition_file_str_by_timestamp(topic_str, partition_int, topic_str_partition_int_timestamp_int_dict_dict[topic_str][partition_int])
                #
                if rel_file_str == -1:
                    offsets_dict[partition_int] = -1
                else:
                    messages_bytes = self.read_bytes(os.path.join(abs_topic_dir_str, "partitions", rel_file_str))
                    #
                    message_bytes_list = messages_bytes.split(b"\n")[:-1]
                    #
                    for message_bytes in message_bytes_list:
                        message_dict = ast.literal_eval(message_bytes.decode("utf-8"))
                        #
                        if message_dict["timestamp"][1] >= offsets_dict[partition_int]:
                            offsets_dict[partition_int] = message_dict["offset"]
                            break
        #
        if replace_not_found_bool:
            topic_str_offsets_dict_dict = self.replace_not_found(topic_str_offsets_dict_dict)
        #
        return topic_str_offsets_dict_dict

    #

    def partitions(self, pattern=None, partitions=None, verbose=False, **kwargs):
        partitions_int = partitions
        #
        topic_str_list = self.list_topics(pattern)
        #
        if partitions_int is not None:
            topic_str_partitions_int_dict = {}
            for topic_str in topic_str_list:
                metadata_dict = self.get_metadata(topic_str)
                #
                metadata_dict["partitions"] = partitions_int
                #
                self.set_metadata(topic_str, metadata_dict)
                #
                topic_str_partitions_int_dict[topic_str] = partitions_int
        #
        topic_str_partitions_int_dict = {topic_str: self.get_partitions(topic_str) for topic_str in topic_str_list}
        #
        return topic_str_partitions_int_dict

    #

    def watermarks(self, pattern, **kwargs):
        topic_str_list = self.list_topics(pattern)
        filtered_topic_str_list = pattern_match(topic_str_list, pattern)
        #
        def get_watermark_offsets(topic_str, partition_int):
            topic_abs_dir_str = self.get_topic_abs_path_str(topic_str)
            rel_file_str_list = self.list_files(os.path.join(topic_abs_dir_str, "partitions"))
            partition_rel_file_str_list = [rel_file_str for rel_file_str in rel_file_str_list if int(rel_file_str.split(",")[0]) == partition_int]
            partition_rel_file_str_list.sort()
            low_offset_int = 0
            high_offset_int = 0
            if len(partition_rel_file_str_list) > 0:
                first_partition_rel_file_str = partition_rel_file_str_list[0]
                last_partition_rel_file_str = partition_rel_file_str_list[-1]
                #
                low_offset_int = int(first_partition_rel_file_str.split(",")[1])
                high_offset_int = int(last_partition_rel_file_str.split(",")[2]) + 1
            #
            return (low_offset_int, high_offset_int)
        #

        topic_str_partition_int_offsets_tuple_dict_dict = {}
        for topic_str in filtered_topic_str_list:
            partitions_int = self.get_partitions(topic_str)
            partition_int_offsets_tuple_dict = {partition_int: get_watermark_offsets(topic_str, partition_int) for partition_int in range(partitions_int)}
            topic_str_partition_int_offsets_tuple_dict_dict[topic_str] = partition_int_offsets_tuple_dict
        #
        return topic_str_partition_int_offsets_tuple_dict_dict

    #
    # File-based topics
    #

    # Topics/Partitions

    def get_abs_path_str(self, rel_path_str):
        abs_path_str = os.path.join(self.storage_obj.root_dir(), rel_path_str)
        #
        return abs_path_str

    def get_topic_abs_path_str(self, topic_str):
        topic_abs_dir_str = os.path.join(self.storage_obj.root_dir(), "topics", topic_str)
        #
        return topic_abs_dir_str

    def get_file_abs_path_str(self, file_str):
        file_abs_file_str = os.path.join(self.storage_obj.root_dir(), "files", file_str)
        #
        return file_abs_file_str

    def find_partition_file_str_by_offset(self, topic_str, partition_int, to_find_offset_int):
        # Get sorted list of all relative file names rel_file_str_list for the partition files for partition_int of topic_str.
        topic_abs_dir_str = self.get_topic_abs_path_str(topic_str)
        rel_file_str_list1 = self.list_files(os.path.join(topic_abs_dir_str, "partitions"))
        rel_file_str_list = [rel_file_str for rel_file_str in rel_file_str_list1 if int(rel_file_str.split(",")[0]) == partition_int]
        if rel_file_str_list == []:
            return None
        rel_file_str_list.sort()
        # Now find the first file in which the message with offset to_find_offset_int is contained :)
        rel_file_str_start_offset_int_end_offset_int_tuple_dict = {rel_file_str: (int(rel_file_str.split(",")[1]), int(rel_file_str.split(",")[2])) for rel_file_str in rel_file_str_list}
        found_rel_file_str = None
        for rel_file_str, start_offset_int_end_offset_int_tuple in rel_file_str_start_offset_int_end_offset_int_tuple_dict.items():
            (start_offset_int, end_offset_int) = start_offset_int_end_offset_int_tuple
            #
            if to_find_offset_int >= start_offset_int and to_find_offset_int <= end_offset_int:
                found_rel_file_str = rel_file_str
                break
        #
        return found_rel_file_str

    def find_partition_file_str_by_timestamp(self, topic_str, partition_int, to_find_timestamp_int):
        # Get sorted list of all relative file names rel_file_str_list for the partition files for partition_int of topic_str.
        topic_abs_dir_str = self.get_topic_abs_path_str(topic_str)
        rel_file_str_list1 = self.list_files(os.path.join(topic_abs_dir_str, "partitions"))
        rel_file_str_list = [rel_file_str for rel_file_str in rel_file_str_list1 if int(rel_file_str.split(",")[0]) == partition_int]
        if rel_file_str_list == []:
            return -1
        #
        # Now find the first file in which the message with timestamp to_find_timestamp_int is contained :)
        rel_file_str_list.sort()
        rel_file_str_start_timestamp_int_end_timestamp_int_tuple_dict = {rel_file_str: (int(rel_file_str.split(",")[3]), int(rel_file_str.split(",")[4])) for rel_file_str in rel_file_str_list}
        # If to_find_timestamp < the minimum timestamp of the files => return the first file
        min_start_timestamp_int = rel_file_str_start_timestamp_int_end_timestamp_int_tuple_dict[rel_file_str_list[0]][0]
        if to_find_timestamp_int < min_start_timestamp_int:
            return rel_file_str_list[0]
        # If to_find_timestamp > the maximum timestamp of the files => return -1
        max_end_timestamp_int = rel_file_str_start_timestamp_int_end_timestamp_int_tuple_dict[rel_file_str_list[-1]][1]
        if to_find_timestamp_int > max_end_timestamp_int:
            return -1
        #
        found_rel_file_str = None
        previous_end_timestamp_int = None
        for rel_file_str, start_timestamp_int_end_timestamp_int_tuple in rel_file_str_start_timestamp_int_end_timestamp_int_tuple_dict.items():
            (start_timestamp_int, end_timestamp_int) = start_timestamp_int_end_timestamp_int_tuple
            #
            if to_find_timestamp_int >= start_timestamp_int and to_find_timestamp_int <= end_timestamp_int:
                found_rel_file_str = rel_file_str
                break
            #
            if previous_end_timestamp_int is not None and to_find_timestamp_int >= previous_end_timestamp_int and to_find_timestamp_int <= end_timestamp_int:
                found_rel_file_str = rel_file_str
                break
            #
            previous_end_timestamp_int = end_timestamp_int
        #
        return found_rel_file_str

    def get_partition_files(self, topic_str):
        topic_abs_dir_str = self.get_topic_abs_path_str(topic_str)
        #
        partitions_int = self.get_partitions(topic_str)
        #
        rel_file_str_list = self.list_files(os.path.join(topic_abs_dir_str, "partitions"))
        #
        def sort(list):
            list.sort()
            return list
        #

        partition_int_rel_file_str_list_dict = {partition_int: sort([rel_file_str for rel_file_str in rel_file_str_list if int(rel_file_str.split(",")[0]) == partition_int]) for partition_int in range(partitions_int)}
        #
        return partition_int_rel_file_str_list_dict

    #

    def delete_groups(self, pattern, state_pattern="*"):
        group_str_list = self.groups(pattern, state_pattern)
        #
        root_dir_str = self.storage_obj.root_dir()
        for group_str in group_str_list:
            group_abs_path_file_str = os.path.join(root_dir_str, "groups", f"{group_str}")
            #
            self.delete_file(group_abs_path_file_str)
        #
        return group_str_list

    def describe_groups(self, pattern="*", state_pattern="*"):
        group_str_state_str_dict = self.groups(pattern, state_pattern, state=True)
        #
        group_str_group_description_dict_dict = {group_str: {"group_id": group_str, "is_simple_consumer_group": False, "partition_assignor": "range", "state": state_str} for group_str, state_str in group_str_state_str_dict.items()}
        #

        return group_str_group_description_dict_dict

    def groups(self, pattern="*", state_pattern="*", state=False):
        state_pattern_str_list = [state_pattern] if isinstance(state_pattern, str) else state_pattern
        state_bool = state
        #
        group_str_list = self.list_groups(pattern)
        #
        group_str_state_str_tuple_list = [(group_str, self.get_group_state(group_str)) for group_str in group_str_list]
        group_str_state_str_tuple_list = [(group_str, state_str) for group_str, state_str in group_str_state_str_tuple_list if any(fnmatch(state_str, state_pattern_str) for state_pattern_str in state_pattern_str_list)]
        #
        if state_bool:
            group_str_state_str_dict = {group_str: state_str for group_str, state_str in group_str_state_str_tuple_list}
            return group_str_state_str_dict
        else:
            group_str_list = [group_str for group_str, _ in group_str_state_str_tuple_list]
            return group_str_list

    def group_offsets(self, pattern, group_offsets=None, state_pattern="*"):
        topic_str_offsets_dict_dict = group_offsets
        #
        group_str_list = self.groups(pattern, state_pattern)
        #
        if topic_str_offsets_dict_dict is not None:
            for group_str in group_str_list:
                new_group_dict = {"offsets": topic_str_offsets_dict_dict}
                self.set_group_dict(group_str, new_group_dict)
        #
        group_str_topic_str_offsets_dict_dict_dict = {group_str: self.get_group_dict(group_str)["offsets"] for group_str in group_str_list}
        #
        return group_str_topic_str_offsets_dict_dict_dict

    # Metadata/Groups

    def read_dict_from_file(self, abs_path_file_str):
        if self.exists_file(abs_path_file_str):
            data_str = self.read_str(abs_path_file_str)
            #
            if data_str is not None:
                data_dict = ast.literal_eval(data_str)
            else:
                data_dict = {}
        else:
            data_dict = {}
        #
        return data_dict

    def write_dict_to_file(self, abs_path_file_str, data_dict):
        data_str = str(data_dict)
        #
        self.write_str(abs_path_file_str, data_str)

    # Metadata

    def get_metadata(self, topic_str):
        topic_dir_str = self.get_topic_abs_path_str(topic_str)
        metadata_dict = self.read_dict_from_file(os.path.join(topic_dir_str, "metadata"))
        #
        return metadata_dict

    def get_partitions(self, topic_str):
        metadata_dict = self.get_metadata(topic_str)
        partitions_int = metadata_dict["partitions"]
        #
        return partitions_int

    def get_config(self, topic_str):
        metadata_dict = self.get_metadata(topic_str)
        config_dict = metadata_dict["config"]
        #
        return config_dict

    def set_metadata(self, topic_str, metadata_dict):
        topic_dir_str = self.get_topic_abs_path_str(topic_str)
        self.write_dict_to_file(os.path.join(topic_dir_str, "metadata"), metadata_dict)

    # Groups

    def get_group_state(self, group_str):
        group_dict = self.get_group_dict(group_str)
        #
        state_str = group_dict["state"]
        #
        return state_str

    def get_group_dict(self, group_str):
        root_dir_str = self.storage_obj.root_dir()
        abs_path_file_str = os.path.join(root_dir_str, "groups", f"{group_str}")
        #
        group_dict = self.read_dict_from_file(abs_path_file_str)
        #
        return group_dict

    def set_group_dict(self, group_str, new_group_dict):
        root_dir_str = self.storage_obj.root_dir()
        abs_path_file_str = os.path.join(root_dir_str, "groups", f"{group_str}")
        #
        group_dict = self.read_dict_from_file(abs_path_file_str)
        #
        if "offsets" in new_group_dict:
            for topic_str, offsets_dict in new_group_dict["offsets"].items():
                if "offsets" not in group_dict:
                    group_dict["offsets"] = {}
                if topic_str not in group_dict["offsets"]:
                    group_dict["offsets"][topic_str] = {}
                #
                for partition_int, offset_int in offsets_dict.items():
                    group_dict["offsets"][topic_str][partition_int] = offset_int
        #
        if "last_update" in new_group_dict:
            group_dict["last_update"] = new_group_dict["last_update"]
        else:
            group_dict["last_update"] = get_millis()
        #
        if "state" in new_group_dict:
            group_dict["state"] = new_group_dict["state"]
        #
        self.write_dict_to_file(abs_path_file_str, group_dict)
        #
        return group_dict
