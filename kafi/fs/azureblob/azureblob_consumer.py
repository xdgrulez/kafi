from kafi.fs.fs_consumer import FSConsumer

#

class AzureBlobConsumer(FSConsumer):
    def __init__(self, azureblob_obj, topic, **kwargs):
        super().__init__(azureblob_obj, topic, **kwargs)

    #

    def close(self):
        return self.topic_str_list
