from kafi.fs.fs import FS
from kafi.fs.azureblob.azureblob_admin import AzureBlobAdmin
from kafi.fs.azureblob.azureblob_consumer import AzureBlobConsumer
from kafi.fs.azureblob.azureblob_producer import AzureBlobProducer

#

class AzureBlob(FS):
    def __init__(self, config_str):
        super().__init__("azureblobs", config_str, ["azure_blob"], [])
    
    #

    def get_admin(self):
        admin = AzureBlobAdmin(self)
        #
        return admin

    #

    def get_consumer(self, file, **kwargs):
        consumer = AzureBlobConsumer(self, file, **kwargs)
        #
        return consumer

    #

    def get_producer(self, file, **kwargs):
        producer = AzureBlobProducer(self, file, **kwargs)
        #
        return producer
