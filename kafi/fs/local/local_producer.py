import os

from kafi.fs.fs_producer import FSProducer


class LocalProducer(FSProducer):
    def __init__(self, local_obj, topic, **kwargs):
        super().__init__(local_obj, topic, **kwargs)

    #

    def close(self):
        return self.topic_str

    #

    def produce_bytes(self, abs_path_file_str, data_bytes):
        os.makedirs(os.path.dirname(abs_path_file_str), exist_ok=True)
        #
        with open(abs_path_file_str, "wb") as bufferedWriter:
            bufferedWriter.write(data_bytes)
