import os

from kafi.fs.fs_writer import FSWriter


class LocalWriter(FSWriter):
    def __init__(self, local_obj, topic, **kwargs):
        super().__init__(local_obj, topic, **kwargs)

    def __del__(self):
        self.close()

    #

    def close(self):
        pass

    #

    def write_bytes(self, path_file_str, bytes, **kwargs):
        with open(path_file_str, "wb") as bufferedWriter:
            bufferedWriter.write(bytes)
