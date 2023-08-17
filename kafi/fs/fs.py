import json
import os

from kafi.storage import Storage

#

class FS(Storage):
    def __init__(self, config_dir_str, config_name_str, mandatory_section_str_list, optional_section_str_list):
        super().__init__(config_dir_str, config_name_str, mandatory_section_str_list, optional_section_str_list)
        #
        self.config_dir_str = config_dir_str
        self.config_name_str = config_name_str
        # azure_blob
        if "azure_blob" in mandatory_section_str_list:
            self.azure_blob_config_dict = self.config_dict["azure_blob"]
            #
            if "container.name" not in self.azure_blob_config_dict:
                self.container_name("test")
            else:
                self.container_name(str(self.azure_blob_config_dict["container.name"]))
        else:
            self.azure_blob_config_dict = None
        # local
        if "local" in mandatory_section_str_list:
            self.local_config_dict = self.config_dict["local"]
            #
            if "root.dir" not in self.local_config_dict:
                self.root_dir(".")
            else:
                self.root_dir(str(self.local_config_dict["root.dir"]))
        else:
            self.local_config_dict = None
        # s3
        if "s3" in mandatory_section_str_list:
            self.s3_config_dict = self.config_dict["s3"]
            #
            if "bucket.name" not in self.s3_config_dict:
                self.bucket_name("minio-test-bucket")
            else:
                self.bucket_name(str(self.s3_config_dict["bucket.name"]))
        else:
            self.s3_config_dict = None
        # all kafi section
        if "message.separator" not in self.kafi_config_dict:
            self.message_separator(b"\n")
        else:
            self.message_separator(bytes(self.kafi_config_dict["message.separator"]))
        #
        self.admin = self.get_admin()

    # azure_blob

    def container_name(self, new_value=None): # str
        return self.get_set_config("container.name", new_value, dict=self.azure_blob_config_dict)

    # local
    
    def root_dir(self, new_value=None): # str
        return self.get_set_config("root.dir", new_value, dict=self.local_config_dict)

    # s3
    
    def bucket_name(self, new_value=None): # str
        return self.get_set_config("bucket.name", new_value, dict=self.s3_config_dict)

    # all

    def message_separator(self, new_value=None): # str
        return self.get_set_config("message.separator", new_value, dict=self.s3_config_dict)

    #

    def topics(self, pattern=None, size=False, **kwargs):
        return self.admin.topics(pattern, size, **kwargs)
    
    ls = topics

    def l(self, pattern=None, size=True, **kwargs):
        return self.admin.topics(pattern=pattern, size=size, **kwargs)

    ll = l

    def exists(self, topic):
        topic_str = topic
        #
        return self.admin.topics(topic_str) != []

    #

    def create(self, topic, partitions=1, **kwargs):
        topic_str = topic
        #
        self.admin.create(topic_str, partitions)
        #
        return topic_str
    
    touch = create

    def delete(self, pattern):
        return self.admin.delete(pattern)

    rm = delete

    def partitions(self, pattern=None, verbose=False):
        return self.admin.partitions(pattern, verbose)

    # Helpers

    def get_topic_dir_str(self, topic_str):
        topic_dir_str = os.path.join(self.root_dir(), f"topic,{topic_str}")
        #
        return topic_dir_str
    
    def get_topic_partition_path_file_str(self, topic_str, partitions_int, partition_int, offset_int):
        topic_dir_str = self.get_topic_dir_str(topic_str, partitions_int)
        #
        partition_path_file_str = os.path.join(f"{topic_dir_str}", f"partition,{partition_int},{offset_int}")
        #
        return partition_path_file_str

    def find_partition_file_str(self, topic_str, partition_int, to_find_offset_int):
        topic_dir_str = self.get_topic_dir_str(topic_str)
        file_str_list = self.admin.list_dir(topic_dir_str)
        partition_file_str_list = [file_str for file_str in file_str_list if file_str.startswith("partition") and int(file_str.split(",")[1]) == partition_int]
        partition_file_str_list.sort()
        #
        partition_file_str_offset_int_dict = {partition_file_str: partition_file_str.split(",")[2] for partition_file_str in partition_file_str_list}
        for partition_file_str, offset_int in partition_file_str_offset_int_dict.items():
            if to_find_offset_int >= offset_int:
                break
        #
        return partition_file_str

    def get_partitions(self, topic_str):
        topic_dir_str = self.get_topic_dir_str(topic_str)
        metadata_dict = self.admin.read_dict_from_file(os.path.join(topic_dir_str, "metadata.json"))
        partitions_int = metadata_dict["partitions"]
        #
        return partitions_int

    def is_topic(self, dir_file_str):
        return dir_file_str.startswith("topic,")

    def get_last_offsets(self, topic_str):
        topic_dir_str = self.get_topic_dir_str(topic_str)
        metadata_dict = self.admin.read_dict_from_file(os.path.join(topic_dir_str, "metadata.json"))
        partitions_int = metadata_dict["partitions"]
        #
        def get_last_offset_of_partition(partition_int):
            file_str_list = self.admin.list_dir(topic_dir_str)
            partition_file_str_list = [file_str for file_str in file_str_list if file_str.startswith("partition") and int(file_str.split(",")[1]) == partition_int]
            #
            if partition_file_str_list == []:
                return 0
            #
            partition_file_str_list.sort()
            #
            last_partition_file_str = partition_file_str_list[-1]
            message_str_list = self.admin.read_lines_from_file(os.path.join(topic_dir_str, last_partition_file_str))
            last_message_str = message_str_list[-1]
            last_message_dict = json.loads(last_message_str)
            last_offset_int = last_message_dict["offset"]
            #
            return last_offset_int
        #

        partition_int_last_offset_int_dict = {partition_int: get_last_offset_of_partition(partition_int) for partition_int in range(partitions_int)}
        #
        return partition_int_last_offset_int_dict

    # Open
    def openr(self, topic, **kwargs):
        reader = self.get_reader(topic, **kwargs)
        #
        return reader
    
    def openw(self, topic, **kwargs):
        writer = self.get_writer(topic, **kwargs)
        #
        return writer
