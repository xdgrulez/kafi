import os
import sys

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test.test_single_storage_base import TestSingleStorageBase
from kafi.fs.local.local import Local

#

class Test(TestSingleStorageBase):
    def setUp(self):
        super().setUp()
        self.path_str = "/tmp/kafi/test/local"
        os.makedirs(self.path_str, exist_ok=True)

    def tearDown(self):
        super().tearDown()

    #

    def get_storage(self):
        # l = Local("local")
        l = Local({"local": {"root.dir": "/tmp"},
                   "schema_registry": {"schema.registry.url": "http://localhost:8081"}})
        l.root_dir(self.path_str)
        #
        return l

    def is_ccloud(self):
        return False

    #

    def test_acls(self):
        pass

    def test_brokers(self):
        pass

    def test_compaction(self):
        pass
