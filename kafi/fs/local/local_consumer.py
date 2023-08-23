from kafi.fs.fs_consumer import FSConsumer

# Constants

ALL_MESSAGES = -1

#

class LocalConsumer(FSConsumer):
    def __init__(self, local_obj, topic, **kwargs):
        super().__init__(local_obj, topic, **kwargs)
    
    #

    def close(self):
        return self.topic_str

    #

    def consume_bytes(self, abs_path_file_str):
        with open(abs_path_file_str, "rb") as bufferedConsumer:
            bytes = bufferedConsumer.read()
        #
        return bytes
