import os
import sys

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test.test_single_storage_base import TestSingleStorageBase
from kafi.fs.s3.s3 import S3

#

class Test(TestSingleStorageBase):
    def setUp(self):
        super().setUp()
        self.path_str = "test"

    def tearDown(self):
        super().tearDown()

    #

    def get_storage(self):
        s = S3("local")
        s.root_dir(self.path_str)
        #
        return s

    def is_ccloud(self):
        return False

    #

    def test_acls(self):
        pass

    def test_brokers(self):
        pass

    def test_compact(self):
        pass
