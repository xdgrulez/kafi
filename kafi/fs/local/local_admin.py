import os

from kafi.fs.fs_admin import FSAdmin

#

class LocalAdmin(FSAdmin):
    def __init__(self, local_obj, **kwargs):
        super().__init__(local_obj, **kwargs)

    # Topics/Files

    def list_dirs(self, abs_path_dir_str):
        collected_rel_dir_str_list = []
        for abs_root_dir_str, rel_dir_str_list, _ in os.walk(abs_path_dir_str):
            rel_root_dir_str = os.path.relpath(abs_root_dir_str, abs_path_dir_str)
            #
            collected_rel_dir_str_list += [rel_dir_str if rel_root_dir_str == "." else os.path.join(rel_root_dir_str, rel_dir_str) for rel_dir_str in rel_dir_str_list]
        #
        collected_rel_dir_str_list.sort()
        #
        return collected_rel_dir_str_list

    def list_files(self, abs_path_dir_str):
        collected_rel_file_str_list = []
        for abs_root_dir_str, _, rel_file_str_list in os.walk(abs_path_dir_str):
            rel_root_dir_str = os.path.relpath(abs_root_dir_str, abs_path_dir_str)
            #
            collected_rel_file_str_list += [rel_file_str if rel_root_dir_str == "." else os.path.join(rel_root_dir_str, rel_file_str) for rel_file_str in rel_file_str_list]
        #
        collected_rel_file_str_list.sort()
        #
        return collected_rel_file_str_list

    def delete_file(self, abs_path_file_str):
        os.remove(abs_path_file_str)

    def delete_dir(self, abs_path_dir_str):
        os.rmdir(abs_path_dir_str)

    def exists_file(self, abs_path_file_str):
        return os.path.exists(abs_path_file_str)

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
