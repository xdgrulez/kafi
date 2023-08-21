from kafi.fs.fs_reader import FSReader

from minio import Minio

#

class S3Reader(FSReader):
    def __init__(self, s3_obj, topic, **kwargs):
        super().__init__(s3_obj, topic, **kwargs)
        #
        self.minio = Minio(s3_obj.s3_config_dict["endpoint"], access_key=s3_obj.s3_config_dict["access.key"], secret_key=s3_obj.s3_config_dict["secret.key"], secure=False)

    #

    def close(self):
        return self.topic_str

    #

    def read_bytes(self, abs_path_file_str):
        response = self.minio.get_object(self.storage_obj.bucket_name(), abs_path_file_str)
        object_bytes = response.data
        #
        return object_bytes
