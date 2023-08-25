import ast
import base64
from fnmatch import fnmatch
import os

from kafi.storage_admin import StorageAdmin
from kafi.helpers import get_millis

class FSAdmin(StorageAdmin):
    def __init__(self, fs_obj, **kwargs):
        super().__init__(fs_obj, **kwargs)
        #
        self.default_state_str = "stable"

    #

    def list_topics(self, pattern=None):
        root_dir_str = self.storage_obj.root_dir()
        rel_dir_str_list = self.list_dirs(root_dir_str)
        #
        topic_str_list = [rel_dir_str.split(",")[1] for rel_dir_str in rel_dir_str_list if self.is_topic(rel_dir_str)]
        #
        filtered_topic_str_list = self.filter_topics(topic_str_list, pattern)
        #
        return filtered_topic_str_list

    #

    def exists(self, topic):
        topic_str = topic
        #
        return self.topics(topic_str) != []

    #

    def partitions(self, pattern=None, verbose=False):
        topic_str_list = self.list_topics(pattern)
        filtered_topic_str_list = self.filter_topics(topic_str_list, pattern)
        #
        topic_str_partitions_int_dict = {}
        for topic_str in filtered_topic_str_list:
            partitions_int = self.get_partitions(topic_str)
            #
            topic_str_partitions_int_dict[topic_str] = partitions_int
        #
        return topic_str_partitions_int_dict

    #

    def create(self, topic, partitions=1, config={}, block=True, **kwargs):
        topic_str = topic
        #
        if self.exists(topic_str):
            raise Exception(f"Topic \"{topic_str}\" already exists.")
        #
        partitions_int = partitions
        #
        message_separator_bytes = kwargs["message_separator"] if "message_separator" in kwargs else self.storage_obj.message_separator()
        message_separator_str = base64.b64encode(message_separator_bytes).decode('utf-8')
        #
        metadata_dict = {"topic": topic_str, "partitions": partitions_int, "message_separator": message_separator_str}
        self.set_metadata(topic_str, metadata_dict)
        #
        self.set_groups(topic_str, {})
    
    #

    def delete(self, pattern, block=True):
        topic_str_list = self.topics(pattern)
        #
        for topic_str in topic_str_list:
            topic_abs_dir_str = self.get_topic_abs_dir_str(topic_str)
            #
            rel_file_str_list = self.list_files(topic_abs_dir_str)
            for rel_file_str in rel_file_str_list:
                self.delete_file(os.path.join(topic_abs_dir_str, rel_file_str))
            #
            self.delete_dir(topic_abs_dir_str)

    #

    def watermarks(self, pattern, **kwargs):
        topic_str_list = self.list_topics(pattern)
        filtered_topic_str_list = self.filter_topics(topic_str_list, pattern)
        #
        def get_watermark_offsets(topic_str, partition_int):
            topic_abs_dir_str = self.get_topic_abs_dir_str(topic_str)
            rel_file_str_list = self.list_files(topic_abs_dir_str)
            partition_rel_file_str_list = [rel_file_str for rel_file_str in rel_file_str_list if rel_file_str.startswith("partition") and int(rel_file_str.split(",")[1]) == partition_int]
            partition_rel_file_str_list.sort()
            low_offset_int = 0
            high_offset_int = 0
            if len(partition_rel_file_str_list) > 0:
                first_partition_rel_file_str = partition_rel_file_str_list[0]
                last_partition_rel_file_str = partition_rel_file_str_list[-1]
                #
                fs_consumer = self.storage_obj.consumer(topic_str)
                low_offset_int = fs_consumer.get_offset_in_partition_file(topic_str, first_partition_rel_file_str, 0)
                high_offset_int = fs_consumer.get_offset_in_partition_file(topic_str, last_partition_rel_file_str, -1) + 1
                fs_consumer.close()
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

    def get_topic_abs_dir_str(self, topic_str):
        topic_abs_dir_str = os.path.join(self.storage_obj.root_dir(), f"topic,{topic_str}")
        #
        return topic_abs_dir_str

    def find_partition_file_str(self, topic_str, partition_int, to_find_offset_int):
        topic_abs_dir_str = self.get_topic_abs_dir_str(topic_str)
        rel_file_str_list1 = self.list_files(topic_abs_dir_str)
        rel_file_str_list = [rel_file_str for rel_file_str in rel_file_str_list1 if rel_file_str.startswith("partition,") and int(rel_file_str.split(",")[1]) == partition_int]
        #
        if rel_file_str_list == []:
            return None
        rel_file_str_list.sort()
        #
        rel_file_str_offset_int_dict = {rel_file_str: int(rel_file_str.split(",")[2]) for rel_file_str in rel_file_str_list}
        for rel_file_str, offset_int in rel_file_str_offset_int_dict.items():
            if to_find_offset_int >= offset_int:
                break
        #
        return rel_file_str

    def get_partition_files(self, topic_str):
        topic_abs_dir_str = self.get_topic_abs_dir_str(topic_str)
        #
        partitions_int = self.get_partitions(topic_str)
        #
        rel_file_str_list = self.list_files(topic_abs_dir_str)
        #
        def sort(list):
            list.sort()
            return list
        #

        partition_int_rel_file_str_list_dict = {partition_int: sort([rel_file_str for rel_file_str in rel_file_str_list if rel_file_str.startswith("partition,") and int(rel_file_str.split(",")[1]) == partition_int]) for partition_int in range(partitions_int)}
        #
        return partition_int_rel_file_str_list_dict

    def is_topic(self, rel_dir_file_str):
        return rel_dir_file_str.startswith("topic,")

    #

    def delete_groups(self, pattern, state_pattern="*"):
        pattern_str_list = [pattern] if isinstance(pattern, str) else pattern
        state_pattern_str_list = [state_pattern] if isinstance(state_pattern, str) else state_pattern
        #
        topic_str_list = self.list_topics()
        #
        group_str_list = []
        for topic_str in topic_str_list:
            group_str_offsets_dict_dict = self.get_groups(topic_str)
            #
            group_str_offsets_dict_dict = {group_str: offsets_dict for group_str, offsets_dict in group_str_offsets_dict_dict.items() if not (any(fnmatch(group_str, pattern_str) for pattern_str in pattern_str_list) and any(fnmatch(self.default_state_str, state_pattern_str) for state_pattern_str in state_pattern_str_list))}
            #
            self.set_groups(topic_str, group_str_offsets_dict_dict)
        #
        return group_str_list

    def describe_groups(self, pattern="*", state_pattern="*"):
        group_str_state_str_dict = self.groups(pattern, state_pattern, state=True)
        #
        group_str_group_description_dict_dict = {group_str: {"group_id": group_str, "is_simple_consumer_group": False} for group_str, state_str in group_str_state_str_dict.items()}
        #
        return group_str_group_description_dict_dict

    def groups(self, pattern="*", state_pattern="*", state=False):
        pattern_str_list = [pattern] if isinstance(pattern, str) else pattern
        state_pattern_str_list = [state_pattern] if isinstance(state_pattern, str) else state_pattern
        state_bool = state
        #
        topic_str_list = self.list_topics()
        #
        group_str_list = []
        for topic_str in topic_str_list:
            group_str_offsets_dict_dict = self.get_groups(topic_str)
            #
            group_str_list += list(group_str_offsets_dict_dict.keys())
        #
        group_str_list = [group_str for group_str in group_str_list if any(fnmatch(group_str, pattern_str) for pattern_str in pattern_str_list) and any(fnmatch(self.default_state_str, state_pattern_str) for state_pattern_str in state_pattern_str_list)]
        #
        if state_bool:
            group_str_state_str_dict = {group_str: self.default_state_str for group_str in group_str_list}
            return group_str_state_str_dict
        else:
            return group_str_list

    def group_offsets(self, pattern, state_pattern="*"):
        pattern_str_list = [pattern] if isinstance(pattern, str) else pattern
        state_pattern_str_list = [state_pattern] if isinstance(state_pattern, str) else state_pattern
        #
        topic_str_list = self.list_topics()
        #
        group_str_topic_str_offsets_dict_dict_dict = {}
        for topic_str in topic_str_list:
            group_str_offsets_dict_dict1 = self.get_groups(topic_str)
            #
            group_str_offsets_dict_dict = {group_str: offsets_dict for group_str, offsets_dict in group_str_offsets_dict_dict1.items() if any(fnmatch(group_str, pattern_str) for pattern_str in pattern_str_list) and any(fnmatch(self.default_state_str, state_pattern_str) for state_pattern_str in state_pattern_str_list)}
            #
            for group_str, offsets_dict in group_str_offsets_dict_dict.items():
                if group_str not in group_str_topic_str_offsets_dict_dict_dict:
                    group_str_topic_str_offsets_dict_dict_dict[group_str] = {topic_str: {}}
                group_str_topic_str_offsets_dict_dict_dict[group_str][topic_str] = offsets_dict
        #
        return group_str_topic_str_offsets_dict_dict_dict

    def set_group_offsets(self, group_offsets):
        group_str_topic_str_offsets_dict_dict_dict = group_offsets
        #
        topic_str_group_str_offsets_dict_dict_dict = {}
        for group_str, topic_str_offsets_dict_dict in group_str_topic_str_offsets_dict_dict_dict.items():
            for topic_str, offsets_dict in topic_str_offsets_dict_dict.items():
                if topic_str not in topic_str_group_str_offsets_dict_dict_dict:
                    topic_str_group_str_offsets_dict_dict_dict[topic_str] = {group_str: {}}
                #
                topic_str_group_str_offsets_dict_dict_dict[topic_str][group_str] = offsets_dict
        #
        for topic_str, group_str_offsets_dict_dict in topic_str_group_str_offsets_dict_dict_dict.items():
            self.set_groups(topic_str, group_str_offsets_dict_dict)
        #
        return group_str_topic_str_offsets_dict_dict_dict

    # Metadata/Groups

    def read_dict_from_file(self, abs_path_file_str):
        data_str = self.read_str(abs_path_file_str)
        #
        if data_str is not None:
            data_dict = ast.literal_eval(data_str)
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
        topic_dir_str = self.get_topic_abs_dir_str(topic_str)
        metadata_dict = self.read_dict_from_file(os.path.join(topic_dir_str, "metadata.json"))
        #
        return metadata_dict

    def get_partitions(self, topic_str):
        metadata_dict = self.get_metadata(topic_str)
        partitions_int = metadata_dict["partitions"]
        #
        return partitions_int

    def get_message_separator(self, topic_str):
        metadata_dict = self.get_metadata(topic_str)
        message_separator_str = metadata_dict["message_separator"]
        message_separator_bytes = bytes(base64.b64decode(message_separator_str))
        #
        return message_separator_bytes

    def set_metadata(self, topic_str, metadata_dict):
        topic_dir_str = self.get_topic_abs_dir_str(topic_str)
        self.write_dict_to_file(os.path.join(topic_dir_str, "metadata.json"), metadata_dict)

    # Groups

    def get_groups(self, topic_str):
        topic_dir_str = self.get_topic_abs_dir_str(topic_str)
        #
        group_str_offsets_dict_last_updated_int_dict_dict = self.read_dict_from_file(os.path.join(topic_dir_str, "groups.json"))
        #
        group_str_offsets_dict_dict = {group_str: offsets_dict_last_updated_int_dict["offsets"] for group_str, offsets_dict_last_updated_int_dict in group_str_offsets_dict_last_updated_int_dict_dict.items()}
        #
        return group_str_offsets_dict_dict

    def set_groups(self, topic_str, set_group_str_offsets_dict_dict):
        topic_dir_str = self.get_topic_abs_dir_str(topic_str)
        #
        group_str_offsets_dict_last_updated_int_dict_dict = self.read_dict_from_file(os.path.join(topic_dir_str, "groups.json"))
        #
        for group_str, offsets_dict in set_group_str_offsets_dict_dict.items():
            for partition_int, offset_int in offsets_dict.items():
                if group_str not in group_str_offsets_dict_last_updated_int_dict_dict:
                    group_str_offsets_dict_last_updated_int_dict_dict[group_str] = {"offsets": {}}
                #
                group_str_offsets_dict_last_updated_int_dict_dict[group_str]["offsets"][partition_int] = offset_int
            #
            group_str_offsets_dict_last_updated_int_dict_dict[group_str]["last_updated"] = get_millis()
        #
        self.write_dict_to_file(os.path.join(topic_dir_str, "groups.json"), group_str_offsets_dict_last_updated_int_dict_dict)
