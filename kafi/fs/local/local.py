from kafi.fs.fs import FS
from kafi.fs.local.local_admin import LocalAdmin
from kafi.fs.local.local_reader import LocalReader
from kafi.fs.local.local_writer import LocalWriter

#

class Local(FS):
    def __init__(self, config_str):
        super().__init__("locals", config_str, ["local"], [])
    
    #

    def get_admin(self):
        reader = LocalAdmin(self)
        #
        return reader

    #

    def get_reader(self, file, **kwargs):
        reader = LocalReader(self, file, **kwargs)
        #
        return reader

    #

    def get_writer(self, file, **kwargs):
        writer = LocalWriter(self, file, **kwargs)
        #
        return writer
