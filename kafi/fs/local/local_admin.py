import os

from kafi.fs.fs_admin import FSAdmin

#

class LocalAdmin(FSAdmin):
    def __init__(self, local_obj, **kwargs):
        super().__init__(local_obj, **kwargs)

    # Topics/Files

    def list_dir(self, abs_path_dir_str):
        rel_dir_file_str_list = os.listdir(abs_path_dir_str)
        #
        return rel_dir_file_str_list

    def list_dir(self, abs_path_dir_str):
        dir_file_str_list = os.listdir(abs_path_dir_str)
        #
        return dir_file_str_list

    def delete_file(self, abs_path_file_str):
        os.remove(abs_path_file_str)

    def delete_dir(self, abs_path_dir_str):
        os.rmdir(abs_path_dir_str)

    # Metadata
    
    def read_str(self, abs_path_file_str):
        with open(abs_path_file_str, "r") as bufferedReader:
            str = bufferedReader.read()
        #
        return str

    def write_str(self, abs_path_file_str, str):
        os.makedirs(os.path.dirname(abs_path_file_str), exist_ok=True)
        #
        with open(abs_path_file_str, "w") as bufferedWriter:
            bufferedWriter.write(str)
