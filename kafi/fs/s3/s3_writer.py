import io

from minio import Minio

from kafi.fs.fs_writer import FSWriter

class S3Writer(FSWriter):
    def __init__(self, s3_obj, file, **kwargs):
        super().__init__(s3_obj, file, **kwargs)
        #
        self.minio = Minio(s3_obj.s3_config_dict["endpoint"], access_key=s3_obj.s3_config_dict["access.key"], secret_key=s3_obj.s3_config_dict["secret.key"], secure=False)

    #

    def close(self):
        return self.topic_str

    #

    def write_bytes(self, abs_path_file_str, data_bytes):
        self.minio.put_object(self.storage_obj.bucket_name(), abs_path_file_str, io.BytesIO(data_bytes), length=len(data_bytes))
