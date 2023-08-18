import os

class FSAdmin:
    def __init__(self, fs_obj):
        self.fs_obj = fs_obj

    #

    def topics(self, pattern=None, size=False, **kwargs):
        pattern_str_or_str_list = "*" if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        size_bool = size
        partitions_bool = "partitions" in kwargs and kwargs["partitions"]
        filesize_bool = "filesize" in kwargs and kwargs["filesize"]
        #
        topic_str_list = self.list_topics(pattern_str_list, partitions=True, filesize=True)
        #
        if size_bool:
            topic_str_size_int_filesize_int_tuple_dict = {topic_str: (self.fs_obj.stat(topic_str), filesize_int) for topic_str, filesize_int in topic_str_filesize_int_dict.items()}
            if partitions_bool:
                if filesize_bool:
                    # e.g. {"topic": {"size": 42, "partitions": {0: 42}, "filesize": 4711}}
                    topic_str_size_int_partitions_dict_filesize_int_dict_dict = {topic_str: {"size": size_int_filesize_int_tuple[0], "partitions": {0: size_int_filesize_int_tuple[0]}, "filesize": size_int_filesize_int_tuple[1]} for topic_str, size_int_filesize_int_tuple in topic_str_size_int_filesize_int_tuple_dict.items()}
                    return topic_str_size_int_partitions_dict_filesize_int_dict_dict
                else:
                    # e.g. {"topic": {"size": 42, "partitions": {0: 42}}}
                    topic_str_size_int_partitions_dict_dict = {topic_str: {"size": size_int_filesize_int_tuple[0], "partitions": {0: size_int_filesize_int_tuple[0]}} for topic_str, size_int_filesize_int_tuple in topic_str_size_int_filesize_int_tuple_dict.items()}
                    return topic_str_size_int_partitions_dict_dict
            else:
                if filesize_bool:
                    # e.g. {"topic": {"size": 42, "filesize": 4711}}
                    topic_str_size_int_filesize_int_dict_dict = {topic_str: {"size": size_int_filesize_int_tuple[0], "filesize": size_int_filesize_int_tuple[1]} for topic_str, size_int_filesize_int_tuple in topic_str_size_int_filesize_int_tuple_dict.items()}
                    return topic_str_size_int_filesize_int_dict_dict
                else:
                    # e.g. {"topic": 42}
                    topic_str_size_int_dict = {topic_str: size_int_filesize_int_tuple[0] for topic_str, size_int_filesize_int_tuple in topic_str_size_int_filesize_int_tuple_dict.items()}
                    return topic_str_size_int_dict
        else:
            if partitions_bool:
                topic_str_size_int_filesize_int_tuple_dict = {topic_str: (self.fs_obj.stat(topic_str), filesize_int) for topic_str, filesize_int in topic_str_filesize_int_dict.items()}
                if filesize_bool:
                    # e.g. {"topic": {"partitions": {0: 42}, "filesize": 4711}}
                    topic_str_partitions_dict_filesize_int_dict_dict = {topic_str: {"partitions": {0: size_int_filesize_int_tuple[0]}, "filesize": size_int_filesize_int_tuple[1]} for topic_str, size_int_filesize_int_tuple in topic_str_size_int_filesize_int_tuple_dict.items()}
                    return topic_str_partitions_dict_filesize_int_dict_dict
                else:
                    # e.g. {"topic": {0: 42}}
                    topic_str_partitions_dict_dict = {topic_str: {0: size_int_filesize_int_tuple[0]} for topic_str, size_int_filesize_int_tuple in topic_str_size_int_filesize_int_tuple_dict.items()}
                    return topic_str_partitions_dict_dict
            else:
                if filesize_bool:
                    # e.g. {"topic": 4711}
                    topic_str_filesize_int_dict = {topic_str: filesize_int for topic_str, filesize_int in topic_str_filesize_int_dict.items()}
                    return topic_str_filesize_int_dict
                else:
                    # e.g. ["topic"]
                    topic_str_list = [topic_str for topic_str in topic_str_filesize_int_dict]
                    return topic_str_list

    #

    def partitions(self, pattern=None, verbose=False):
        topic_str_list = self.topics(pattern)
        #
        topic_str_num_partitions_int_dict = {topic_str: 1 for topic_str in topic_str_list}
        #
        return topic_str_num_partitions_int_dict

    #

    def create(self, topic, partitions=1, config={}, block=True):
        topic_str = topic
        partitions_int = partitions
        #
        topic_dir_str = self.fs_obj.get_topic_dir_str(topic_str)
        #
        metadata_dict = {"topic": topic_str, "partitions": partitions_int}
        self.write_dict_to_file(os.path.join(topic_dir_str, "metadata.json"), metadata_dict)
    
    #

    def delete(self, pattern, block=True):
        topic_str_list = self.list_topics(pattern)
        #
        for topic_str in topic_str_list:
            topic_dir_str = self.fs_obj.get_topic_dir_str(topic_str)
            #
            file_str_list = self.list_dir(topic_dir_str)
            for file_str in file_str_list:
                self.delete_file(os.path.join(topic_dir_str, file_str))
            #
            self.delete_dir(topic_dir_str)
