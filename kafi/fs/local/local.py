from kafi.fs.fs import FS
from kafi.fs.local.local_admin import LocalAdmin
from kafi.fs.local.local_consumer import LocalConsumer
from kafi.fs.local.local_producer import LocalProducer

#

class Local(FS):
    def __init__(self, config_str_or_dict):
        super().__init__("locals", config_str_or_dict, ["local"], ["schema_registry"])
        #
        self.local_config_dict = self.config_dict["local"]
        #
        if "root.dir" not in self.local_config_dict:
            self.root_dir(".")
        else:
            self.root_dir(str(self.local_config_dict["root.dir"]))
        #
        self.admin = self.get_admin()

    #

    def root_dir(self, new_value=None): # str
        return self.get_set_config("root.dir", new_value, dict=self.local_config_dict)

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
    