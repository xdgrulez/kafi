import os

from kafi.fs.fs_writer import FSWriter


class LocalWriter(FSWriter):
    def __init__(self, local_obj, topic, **kwargs):
        super().__init__(local_obj, topic, **kwargs)

    #

    def close(self):
        return self.topic_str

    #

    def write_bytes(self, abs_path_file_str, bytes):
        os.makedirs(os.path.dirname(abs_path_file_str), exist_ok=True)
        #
        with open(abs_path_file_str, "wb") as bufferedWriter:
            bufferedWriter.write(bytes)
