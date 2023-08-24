import os

from kafi.fs.fs_admin import FSAdmin

#

class LocalAdmin(FSAdmin):
    def __init__(self, local_obj, **kwargs):
        super().__init__(local_obj, **kwargs)

    # Topics/Files

    def list_dirs(self, abs_path_dir_str):
        rel_dir_file_str_list = os.listdir(abs_path_dir_str)
        rel_dir_str_list = [rel_dir_file_str for rel_dir_file_str in rel_dir_file_str_list if os.path.isdir(os.path.join(abs_path_dir_str, rel_dir_file_str))]
        rel_dir_str_list.sort()
        #
        return rel_dir_str_list

    def list_files(self, abs_path_dir_str):
        rel_dir_file_str_list = os.listdir(abs_path_dir_str)
        rel_file_str_list = [rel_dir_file_str for rel_dir_file_str in rel_dir_file_str_list if os.path.isfile(os.path.join(abs_path_dir_str, rel_dir_file_str))]
        rel_file_str_list.sort()
        #
        return rel_file_str_list

    def delete_file(self, abs_path_file_str):
        os.remove(abs_path_file_str)

    def delete_dir(self, abs_path_dir_str):
        os.rmdir(abs_path_dir_str)

    # Metadata
    
    def read_str(self, abs_path_file_str):
        str = None
        #
        if os.path.exists(abs_path_file_str):
            with open(abs_path_file_str, "r") as bufferedReader:
                str = bufferedReader.read()
        #
        return str

    def write_str(self, abs_path_file_str, data_str):
        os.makedirs(os.path.dirname(abs_path_file_str), exist_ok=True)
        #
        with open(abs_path_file_str, "w") as bufferedWriter:
            bufferedWriter.write(data_str)
