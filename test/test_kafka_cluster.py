import os
import sys

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test.test_single_storage_base import TestSingleStorageBase
from kafi.kafka.cluster.cluster import Cluster

#

class Test(TestSingleStorageBase):
    def setUp(self):
        super().setUp()
        self.principal_str = None
        # self.principal_str = "User:admin"


    def tearDown(self):
        super().tearDown()

#

    def get_storage(self):
        c = Cluster("local")
#        c = Cluster("ccloud")
        #
        return c

    def is_ccloud(self):
        c = self.get_storage()
        return "confluent.cloud" in c.kafka_config_dict["bootstrap.servers"]
