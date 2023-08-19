from kafi.fs.fs_reader import FSReader

# Constants

ALL_MESSAGES = -1

#

class LocalReader(FSReader):
    def __init__(self, local_obj, file, **kwargs):
        super().__init__(local_obj, file, **kwargs)
    
    #

    def close(self):
        return self.topic_str

    #

    def read_bytes(self, abs_path_file_str):
        with open(abs_path_file_str, "rb") as bufferedReader:
            bytes = bufferedReader.read()
        #
        return bytes
