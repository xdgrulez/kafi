from kafi.fs.fs_producer import FSProducer

class S3Producer(FSProducer):
    def __init__(self, s3_obj, file, **kwargs):
        super().__init__(s3_obj, file, **kwargs)

    #

    def close(self):
        return self.topic_str

