import io
import os

from kafi.fs.fs_admin import FSAdmin

from minio import Minio

#

class S3Admin(FSAdmin):
    def __init__(self, s3_obj):
        super().__init__(s3_obj)
        #
        self.minio = Minio(s3_obj.s3_config_dict["endpoint"], access_key=s3_obj.s3_config_dict["access.key"], secret_key=s3_obj.s3_config_dict["secret.key"], secure=False)

    # Topics/Files

    def list_dirs(self, abs_path_dir_str):
        object_generator = self.minio.list_objects(self.storage_obj.bucket_name(), prefix=abs_path_dir_str, recursive=True)
        rel_dir_str_list = [os.path.basename(os.path.dirname(object.object_name)) for object in object_generator]
        #
        rel_dir_str_list.sort()
        #
        return rel_dir_str_list

    def list_files(self, abs_path_dir_str):
        object_generator = self.minio.list_objects(self.storage_obj.bucket_name(), prefix=abs_path_dir_str, recursive=True)
        rel_file_str_list = [os.path.basename(object.object_name) for object in object_generator]
        #
        rel_file_str_list.sort()
        #
        return rel_file_str_list

    def delete_file(self, abs_path_file_str):
        self.minio.remove_object(self.storage_obj.bucket_name(), abs_path_file_str)

    def delete_dir(self, _):
        pass

    # Metadata
    
    def consume_str(self, abs_path_file_str):
        response = self.minio.get_object(self.storage_obj.bucket_name(), abs_path_file_str)
        object_bytes = response.data
        #
        object_str = object_bytes.decode("utf-8")
        #
        return object_str

    def produce_str(self, abs_path_file_str, data_str):
        data_bytes = data_str.encode("utf-8")
        #
        self.minio.put_object(self.storage_obj.bucket_name(), abs_path_file_str, io.BytesIO(data_bytes), length=len(data_bytes))
