from kafi.fs.fs_producer import FSProducer


class AzureBlobProducer(FSProducer):
    def __init__(self, azureblob_obj, file, **kwargs):
        super().__init__(azureblob_obj, file, **kwargs)

    #

    def close(self):
        return self.topic_str
