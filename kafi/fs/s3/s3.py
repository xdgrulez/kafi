from kafi.fs.fs import FS
from kafi.fs.s3.s3_admin import S3Admin
from kafi.fs.s3.s3_consumer import S3Consumer
from kafi.fs.s3.s3_producer import S3Producer

#

class S3(FS):
    def __init__(self, config_str_or_dict):
        super().__init__("s3s", config_str_or_dict, ["s3"], ["schema_registry"])
        #
        self.s3_config_dict = self.config_dict["s3"]
        #
        if "access.key" not in self.s3_config_dict:
            self.access_key("")
        else:
            self.access_key(str(self.s3_config_dict["access.key"]))
        #
        if "bucket.name" not in self.s3_config_dict:
            self.bucket_name("test")
        else:
            self.bucket_name(str(self.s3_config_dict["bucket.name"]))
        #
        if "endpoint" not in self.s3_config_dict:
            self.endpoint("")
        else:
            self.endpoint(str(self.s3_config_dict["endpoint"]))
        #
        if "root.dir" not in self.s3_config_dict:
            self.root_dir(".")
        else:
            self.root_dir(str(self.s3_config_dict["root.dir"]))
        #
        if "secret.key" not in self.s3_config_dict:
            self.secret_key("")
        else:
            self.secret_key(str(self.s3_config_dict["secret.key"]))
        #
        self.admin = self.get_admin()

    #

    def access_key(self, new_value=None): # str
        return self.get_set_config("access.key", new_value, dict=self.s3_config_dict)

    def bucket_name(self, new_value=None): # str
        return self.get_set_config("bucket.name", new_value, dict=self.s3_config_dict)

    def endpoint(self, new_value=None): # str
        return self.get_set_config("endpoint", new_value, dict=self.s3_config_dict)

    def root_dir(self, new_value=None): # str
        return self.get_set_config("root.dir", new_value, dict=self.s3_config_dict)

    def secret_key(self, new_value=None): # str
        return self.get_set_config("secret.key", new_value, dict=self.s3_config_dict)

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
