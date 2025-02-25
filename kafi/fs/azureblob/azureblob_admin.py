import os

from kafi.fs.fs_admin import FSAdmin

#

class AzureBlobAdmin(FSAdmin):
    def __init__(self, azureblob_obj):
        from azure.storage.blob import BlobServiceClient
        #

        super().__init__(azureblob_obj)
        #
        blobServiceClient = BlobServiceClient.from_connection_string(azureblob_obj.azure_blob_config_dict["connection.string"])
        self.containerClient = blobServiceClient.get_container_client(azureblob_obj.container_name())

    # Topics/Files

    def list_dirs(self, abs_path_dir_str):
        blob_str_itemPaged = self.containerClient.list_blob_names(name_starts_with=abs_path_dir_str)
        rel_dir_str_set = set([os.path.basename(os.path.dirname(blob_str)) for blob_str in blob_str_itemPaged])
        #
        rel_dir_str_list = list(rel_dir_str_set)
        rel_dir_str_list.sort()
        #
        return rel_dir_str_list

    def list_files(self, abs_path_dir_str):
        blob_str_itemPaged = self.containerClient.list_blob_names(name_starts_with=abs_path_dir_str)
        rel_file_str_set = set([os.path.relpath(blob_str, abs_path_dir_str) for blob_str in blob_str_itemPaged])
        #
        rel_file_str_list = list(rel_file_str_set)
        rel_file_str_list.sort()
        #
        return rel_file_str_list


    def delete_file(self, abs_path_file_str):
        self.containerClient.delete_blob(abs_path_file_str)

    def delete_dir(self, _):
        pass

    def exists_file(self, abs_path_file_str):
        from azure.storage.blob import BlobClient
        #

        blobClient = BlobClient.from_connection_string(self.storage_obj.azure_blob_config_dict["connection.string"], container_name=self.storage_obj.azure_blob_config_dict["container.name"], blob_name=abs_path_file_str)
        #
        return blobClient.exists()

    # Metadata
    
    def read_str(self, abs_path_file_str):
        from azure.storage.blob import BlobClient
        #

        blobClient = BlobClient.from_connection_string(conn_str=self.storage_obj.azure_blob_config_dict["connection.string"], container_name=self.storage_obj.azure_blob_config_dict["container.name"], blob_name=abs_path_file_str)
        #
        storageStreamDownloader = blobClient.download_blob()
        blob_bytes = storageStreamDownloader.read()
        #
        blob_str = blob_bytes.decode("utf-8")
        #
        return blob_str

    def write_str(self, abs_path_file_str, data_str):
        from azure.storage.blob import BlobClient
        #

        blobClient = BlobClient.from_connection_string(conn_str=self.storage_obj.azure_blob_config_dict["connection.string"], container_name=self.storage_obj.azure_blob_config_dict["container.name"], blob_name=abs_path_file_str)
        #
        data_bytes = data_str.encode("utf-8")
        #
        blobClient.upload_blob(data_bytes, overwrite=True)

    #

    def read_bytes(self, abs_path_file_str):
        from azure.storage.blob import BlobClient
        #

        blobClient = BlobClient.from_connection_string(conn_str=self.storage_obj.azure_blob_config_dict["connection.string"], container_name=self.storage_obj.azure_blob_config_dict["container.name"], blob_name=abs_path_file_str)
        #
        storageStreamDownloader = blobClient.download_blob()
        blob_bytes = storageStreamDownloader.read()
        #
        return blob_bytes

    def write_bytes(self, abs_path_file_str, data_bytes):
        from azure.storage.blob import BlobClient
        #

        blobClient = BlobClient.from_connection_string(conn_str=self.storage_obj.azure_blob_config_dict["connection.string"], container_name=self.storage_obj.azure_blob_config_dict["container.name"], blob_name=abs_path_file_str)
        #
        blobClient.upload_blob(data_bytes)
