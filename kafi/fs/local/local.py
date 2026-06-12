from kafi.fs.fs import FS
from kafi.fs.local.local_admin import LocalAdmin
from kafi.fs.local.local_consumer import LocalConsumer
from kafi.fs.local.local_producer import LocalProducer

#

class Local(FS):
    def __init__(self, config_str_or_dict):
        super().__init__("locals", config_str_or_dict, ["local"], ["schema_registry"])
    
    #

    def get_admin(self):
        consumer = LocalAdmin(self)
        #
        return consumer

    #

    def get_consumer(self, file, **kwargs):
        consumer = LocalConsumer(self, file, **kwargs)
        #
        return consumer

    #

    def get_producer(self, file, **kwargs):
        producer = LocalProducer(self, file, **kwargs)
        #
        return producer
    