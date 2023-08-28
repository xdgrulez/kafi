from kafi.fs.fs_consumer import FSConsumer

#

class S3Consumer(FSConsumer):
    def __init__(self, s3_obj, topic, **kwargs):
        super().__init__(s3_obj, topic, **kwargs)

    #

    def close(self):
        return self.topic_str_list
