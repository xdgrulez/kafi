import os
import tempfile

from azure.storage.blob import BlobClient

from kafi.fs.fs_writer import FSWriter


class AzureBlobWriter(FSWriter):
    def __init__(self, azureblob_obj, file, **kwargs):
        super().__init__(azureblob_obj, file, **kwargs)

    #

    def close(self):
        return self.topic_str

    #

    def write_bytes(self, abs_path_file_str, data_bytes):
        blobClient = BlobClient.from_connection_string(conn_str=self.storage_obj.azure_blob_config_dict["connection.string"], container_name=self.storage_obj.azure_blob_config_dict["container.name"], blob_name=abs_path_file_str)
        #
        blobClient.upload_blob(data_bytes)
