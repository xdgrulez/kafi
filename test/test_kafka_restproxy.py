import os
import sys

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test.test_single_storage_base import TestSingleStorageBase
from kafi.kafka.restproxy.restproxy import RestProxy
from kafi.kafka.cluster.cluster import Cluster

class Test(TestSingleStorageBase):
    def setUp(self):
        super().setUp()
        self.principal_str = None
        # self.principal_str = "User:admin" # also comment out s = self.get_cluster() in test_single_storage_base.py if using the REST Proxy to Confluent Cloud


    def tearDown(self):
        super().tearDown()

    #

    def get_storage(self):
        r = RestProxy("local")
        #
        return r

    def get_cluster(self):
        c = Cluster("local")
        #
        return c

    def is_ccloud(self):
        r = self.get_storage()
        #
        return "confluent.cloud" in r.rest_proxy_config_dict["rest.proxy.url"]
    
    def test_delete_groups(self):
        pass

    def test_set_group_offsets(self):
        pass

    def test_lags(self):
        pass

    def test_offsets_for_times(self):
        pass

    def test_consume_from_offsets(self):
        pass

    def test_error_handling(self):
        pass

    def test_offsets_cache(self):
        pass

    def test_compaction(self):
        pass

    def test_cp_group_offsets(self):
        pass

    def test_chunking(self):
        pass
