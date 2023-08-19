import ast
import base64
import os

from kafi.storage_admin import StorageAdmin

class FSAdmin(StorageAdmin):
    def __init__(self, fs_obj, **kwargs):
        super().__init__(fs_obj, **kwargs)

    #

    def partitions(self, pattern=None, verbose=False):
        topic_str_list = self.topics(pattern)
        #
        topic_str_num_partitions_int_dict = {topic_str: 1 for topic_str in topic_str_list}
        #
        return topic_str_num_partitions_int_dict

    #

    def create(self, topic, partitions=1, config={}, block=True, **kwargs):
        topic_str = topic
        partitions_int = partitions
        #
        topic_dir_str = self.get_topic_dir_str(topic_str)
        #
        message_separator_bytes = kwargs["message_separator"] if "message_separator" in kwargs else self.storage_obj.message_separator()
        message_separator_str = base64.b64encode(message_separator_bytes).decode('utf-8')
        #
        metadata_dict = {"topic": topic_str, "partitions": partitions_int, "message_separator": message_separator_str}
        self.write_dict_to_file(os.path.join(topic_dir_str, "metadata.json"), metadata_dict)
    
    #

    def delete(self, pattern, block=True):
        topic_str_list = self.list_topics(pattern)
        #
        for topic_str in topic_str_list:
            topic_dir_str = self.get_topic_dir_str(topic_str)
            #
            file_str_list = self.list_dir(topic_dir_str)
            for file_str in file_str_list:
                self.delete_file(os.path.join(topic_dir_str, file_str))
            #
            self.delete_dir(topic_dir_str)

    #

    def watermarks(self, pattern, **kwargs):
        topic_str_list = self.list_topics(pattern)
        #
        def get_watermark_offsets(topic_str, partition_int):
            topic_dir_str = self.get_topic_dir_str(topic_str)
            file_str_list = self.list_dir(topic_dir_str)
            partition_file_str_list = [file_str for file_str in file_str_list if file_str.startswith("partition") and int(file_str.split(",")[1]) == partition_int]
            partition_file_str_list.sort()
            low_offset_int = 0
            high_offset_int = 0
            if len(partition_file_str_list) > 0:
                first_partition_file_str = partition_file_str_list[0]
                last_partition_file_str = partition_file_str_list[-1]
                #
                low_offset_int = self.storage_obj.admin.get_offset_in_partition_file(topic_str, first_partition_file_str, 0)
                high_offset_int = self.storage_obj.admin.get_offset_in_partition_file(topic_str, last_partition_file_str, -1) + 1
            #
            return (low_offset_int, high_offset_int)
        #

        topic_str_partition_int_offsets_tuple_dict_dict = {}
        for topic_str in topic_str_list:
            partitions_int = self.get_partitions(topic_str)
            partitions_int = self.partitions(topic_str)[topic_str]
            partition_int_offsets_tuple_dict = {partition_int: get_watermark_offsets(topic_str, partition_int) for partition_int in range(partitions_int)}
            topic_str_partition_int_offsets_tuple_dict_dict[topic_str] = partition_int_offsets_tuple_dict
        #
        return topic_str_partition_int_offsets_tuple_dict_dict

    #
    # File-based topics
    #

    # Topics/Partitions

    def get_topic_dir_str(self, topic_str):
        topic_dir_str = os.path.join(self.storage_obj.root_dir(), f"topic,{topic_str}")
        #
        return topic_dir_str

    def find_partition_file_str(self, topic_str, partition_int, to_find_offset_int):
        topic_dir_str = self.get_topic_dir_str(topic_str)
        file_str_list = self.list_dir(topic_dir_str)
        partition_file_str_list = [file_str for file_str in file_str_list if file_str.startswith("partition,") and int(file_str.split(",")[1]) == partition_int]
        partition_file_str_list.sort()
        #
        partition_file_str_offset_int_dict = {partition_file_str: int(partition_file_str.split(",")[2]) for partition_file_str in partition_file_str_list}
        for partition_file_str, offset_int in partition_file_str_offset_int_dict.items():
            if to_find_offset_int >= offset_int:
                break
        #
        return partition_file_str

    def get_partition_files(self, topic_str):
        topic_dir_str = self.get_topic_dir_str(topic_str)
        #
        partitions_int = self.get_partitions(topic_str)
        #
        file_str_list = self.list_dir(topic_dir_str)
        #
        def sort(list):
            list.sort()
            return list
        #

        partition_int_file_str_list_dict = {partition_int: sort([file_str for file_str in file_str_list if file_str.startswith("partition,") and int(file_str.split(",")[1]) == partition_int]) for partition_int in range(partitions_int)}
        #
        return partition_int_file_str_list_dict

    def is_topic(self, dir_file_str):
        return dir_file_str.startswith("topic,")

    def get_offset_in_partition_file(self, topic_str, partition_file_str, index_int):
        topic_dir_str = self.get_topic_dir_str(topic_str)
        message_separator_bytes = self.get_message_separator(topic_str)
        #
        file_bytes = self.read_bytes_from_file(os.path.join(topic_dir_str, partition_file_str))
        message_bytes_list = file_bytes.split(message_separator_bytes)[:-1]
        if len(message_bytes_list) > 0:
            first_message_bytes = message_bytes_list[index_int]
            serialized_message_dict = ast.literal_eval(first_message_bytes.decode("utf-8"))
            offset_int = serialized_message_dict["offset"]
        return offset_int

    # Metadata

    def get_metadata(self, topic_str):
        topic_dir_str = self.get_topic_dir_str(topic_str)
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
