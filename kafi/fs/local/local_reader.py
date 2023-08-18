import os

from kafi.fs.fs_reader import FSReader

# Constants

ALL_MESSAGES = -1

#

class LocalReader(FSReader):
    def __init__(self, local_obj, file, **kwargs):
        super().__init__(local_obj, file, **kwargs)
    
    #

    def __del__(self):
        self.close()

    #

    def close(self):
        return self.topic_str

    #

    def read_bytes(self, n_int, file_offset_int=0, **kwargs):
        if file_offset_int > 0:
            self.bufferedReader.seek(file_offset_int)
        #
        batch_bytes = self.bufferedReader.read(n_int)
        #
        return batch_bytes
