from kafi.storage import Storage

#

class FS(Storage):
    def __init__(self, config_dir_str, config_name_str, mandatory_section_str_list, optional_section_str_list):
        super().__init__(config_dir_str, config_name_str, mandatory_section_str_list, optional_section_str_list)
        #
        self.config_dir_str = config_dir_str
        self.config_name_str = config_name_str
        # local, azure_blob and s3
        if "local" in mandatory_section_str_list:
            self.local_config_dict = self.config_dict["local"]
            #
            if "root.dir" not in self.local_config_dict:
                self.root_dir(".")
            else:
                self.root_dir(str(self.local_config_dict["root.dir"]))
        else:
            self.local_config_dict = None
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

    # Topics

    def topics(self, pattern=None, size=False, **kwargs):
        return self.admin.topics(pattern, size, **kwargs)
    
    ls = topics

    def l(self, pattern=None, size=True, **kwargs):
        return self.admin.topics(pattern=pattern, size=size, **kwargs)

    ll = l

    def exists(self, topic):
        return self.admin.exists(topic)

    #

    def watermarks(self, pattern, **kwargs):
        return self.admin.watermarks(pattern, **kwargs)

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

    def set_partitions(self, pattern, num_partitions, test=False):
        return self.admin.set_partitions(pattern, num_partitions, test)

    # Groups

    def groups(self, pattern="*", state_pattern="*", state=False):
        return self.admin.groups(pattern, state_pattern, state)
    
    def describe_groups(self, pattern="*", state_pattern="*"):
        return self.admin.describe_groups(pattern, state_pattern)
    
    def delete_groups(self, pattern, state_pattern="*"):
        return self.admin.delete_groups(pattern, state_pattern)
    
    def group_offsets(self, pattern, state_pattern="*"):
        return self.admin.group_offsets(pattern, state_pattern)

    def set_group_offsets(self, group_offsets):
        return self.admin.set_group_offsets(group_offsets)

    # Open
    def consumer(self, topic, **kwargs):
        consumer = self.get_consumer(topic, **kwargs)
        #
        return consumer
    
    def producer(self, topic, **kwargs):
        producer = self.get_producer(topic, **kwargs)
        #
        return producer
