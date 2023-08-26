from azure.storage.blob import BlobClient

from kafi.fs.fs_consumer import FSConsumer

# Constants

ALL_MESSAGES = -1

#

class AzureBlobConsumer(FSConsumer):
    def __init__(self, azureblob_obj, topic, **kwargs):
        super().__init__(azureblob_obj, topic, **kwargs)

    #

    def close(self):
        return self.topic_str_list

    #

    def consume_bytes(self, abs_path_file_str):
        blobClient = BlobClient.from_connection_string(conn_str=self.storage_obj.azure_blob_config_dict["connection.string"], container_name=self.storage_obj.azure_blob_config_dict["container.name"], blob_name=abs_path_file_str)
        #
        storageStreamDownloader = blobClient.download_blob()
        blob_bytes = storageStreamDownloader.read()
        #
        return blob_bytes
