from kafi.fs.fs import FS
from kafi.fs.azureblob.azureblob_admin import AzureBlobAdmin
from kafi.fs.azureblob.azureblob_consumer import AzureBlobConsumer
from kafi.fs.azureblob.azureblob_producer import AzureBlobProducer

#

class AzureBlob(FS):
    def __init__(self, config_str_or_dict):
        super().__init__("azureblobs", config_str_or_dict, ["azure_blob"], ["schema_registry"])
        #
        self.azure_blob_config_dict = self.config_dict["azure_blob"]
        #
        if "connection.string" not in self.azure_blob_config_dict:
            self.connection_string(".")
        else:
            self.connection_string(str(self.azure_blob_config_dict["connection.string"]))
        #
        if "container.name" not in self.azure_blob_config_dict:
            self.container_name("test")
        else:
            self.container_name(str(self.azure_blob_config_dict["container.name"]))
        #
        if "root.dir" not in self.azure_blob_config_dict:
            self.root_dir(".")
        else:
            self.root_dir(str(self.azure_blob_config_dict["root.dir"]))
        #
        self.admin = self.get_admin()

    #

    def connection_string(self, new_value=None): # str
        return self.get_set_config("connection.string", new_value, dict=self.azure_blob_config_dict)

    def container_name(self, new_value=None): # str
        return self.get_set_config("container.name", new_value, dict=self.azure_blob_config_dict)

    def root_dir(self, new_value=None): # str
        return self.get_set_config("root.dir", new_value, dict=self.azure_blob_config_dict)

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
