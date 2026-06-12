from kafi.fs.fs import FS
from kafi.fs.s3.s3_admin import S3Admin
from kafi.fs.s3.s3_consumer import S3Consumer
from kafi.fs.s3.s3_producer import S3Producer

#

class S3(FS):
    def __init__(self, config_str_or_dict):
        super().__init__("s3s", config_str_or_dict, ["s3"], ["schema_registry"])
    
    #

    def get_admin(self):
        consumer = S3Admin(self)
        #
        return consumer

    #

    def get_consumer(self, file, **kwargs):
        consumer = S3Consumer(self, file, **kwargs)
        #
        return consumer

    #

    def get_producer(self, file, **kwargs):
        producer = S3Producer(self, file, **kwargs)
        #
        return producer
