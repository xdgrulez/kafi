import os
import sys

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test.test_single_storage_base import TestSingleStorageBase
from kafi.fs.azureblob.azureblob import AzureBlob

#

class Test(TestSingleStorageBase):
    def setUp(self):
        super().setUp()
        self.path_str = "test"

    def tearDown(self):
        super().tearDown()

    #

    def get_storage(self):
        a = AzureBlob("local")
        a.root_dir(self.path_str)
        #
        return a

    def is_ccloud(self):
        return False

    def test_acls(self):
        pass

    def test_brokers(self):
        pass

    def test_offsets_for_times(self):
        pass
