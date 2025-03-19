from kafi.fs.fs_consumer import FSConsumer

# Constants

ALL_MESSAGES = -1

#

class LocalConsumer(FSConsumer):
    def __init__(self, local_obj, topic, **kwargs):
        super().__init__(local_obj, topic, **kwargs)
