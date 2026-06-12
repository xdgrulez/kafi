from kafi.fs.fs_producer import FSProducer


class LocalProducer(FSProducer):
    def __init__(self, local_obj, topic, **kwargs):
        super().__init__(local_obj, topic, **kwargs)

    #

    def close(self):
        return self.topic_str

