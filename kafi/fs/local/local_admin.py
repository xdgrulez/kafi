from fnmatch import fnmatch
import json
import os

from kafi.fs.fs_admin import FSAdmin

#

class LocalAdmin(FSAdmin):
    def __init__(self, local_obj):
        super().__init__(local_obj)

    #

    def list_topics(self, pattern=None):
        pattern_str_or_str_list = pattern
        #
        dir_file_str_list = os.listdir(self.fs_obj.root_dir())
        topic_str_list = [dir_file_str.split(",")[1] for dir_file_str in dir_file_str_list if self.fs_obj.is_topic(dir_file_str)]
        #
        if pattern_str_or_str_list is not None:
            if isinstance(pattern_str_or_str_list, str):
                pattern_str_or_str_list = [pattern_str_or_str_list]
            #
            topic_str_list = [topic_str for topic_str in topic_str_list if any(fnmatch(topic_str, pattern_str) for pattern_str in pattern_str_or_str_list)]
        #
        return topic_str_list

    #

    def list_dir(self, path_dir_str):
        dir_file_str_list = os.listdir(path_dir_str)
        #
        return dir_file_str_list

    def write_dict_to_file(self, path_file_str, dict):
        os.makedirs(os.path.dirname(path_file_str), exist_ok=True)
        #
        with open(path_file_str, "w") as bufferedWriter:
            bufferedWriter.write(json.dumps(dict))
    
    def read_dict_from_file(self, path_file_str):
        with open(path_file_str, "r") as bufferedReader:
            str = bufferedReader.read()
        #
        return json.loads(str)

    def read_lines_from_file(self, path_file_str):
        with open(path_file_str, "r") as bufferedReader:
            str_list = bufferedReader.read().splitlines()
        #
        return str_list

    def read_bytes_from_file(self, path_file_str):
        with open(path_file_str, "rb") as bufferedReader:
            bytes = bufferedReader.read()
        #
        return bytes

    def delete_file(self, path_file_str):
        os.remove(path_file_str)

    def delete_dir(self, path_dir_str):
        os.rmdir(path_dir_str)
