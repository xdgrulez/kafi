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
    
    ### RestProxyAdmin
    # ACLs

    def test_acls(self):
        super().test_acls()

    # Brokers
    
    def test_brokers(self):
        super().test_brokers()

    # Groups

    def test_groups(self):
        super().test_groups()

    def test_describe_groups(self):
        super().test_describe_groups()

    def test_delete_groups(self):
        pass

    def test_group_offsets(self):
        super().test_group_offsets()

    def test_set_group_offsets(self):
        pass

    # Topics

    def test_config_set_config(self):
        super().test_config_set_config()

    def test_create_delete(self):
        super().test_create_delete()

    def test_topics(self):
        super().test_topics()

    def test_offsets_for_times(self):
        pass

    def test_partitions_set_partitions(self):
        super().test_partitions_set_partitions()

    def test_exists(self):
        super().test_exists()

    # Produce/Consume

    def test_produce_consume_bytes_str(self):
        super().test_produce_consume_bytes_str()
    
    def test_produce_consume_json(self):
        super().test_produce_consume_json()

    def test_produce_consume_protobuf(self):
        super().test_produce_consume_protobuf()

    def test_produce_consume_avro(self):
        super().test_produce_consume_avro()

    def test_produce_consume_jsonschema(self):
        super().test_produce_consume_jsonschema()

    def test_consume_from_offsets(self):
        pass

    def test_commit(self):
        super().test_commit()
    
    def test_error_handling(self):
        pass

    def test_cluster_settings(self):
        super().test_cluster_settings()

    def test_configs(self):
        super().test_configs()

    # Shell

    def test_cat(self):
        super().test_cat()

    def test_head(self):
        super().test_head()

    def test_tail(self):
        super().test_tail()

    def test_cp(self):
        super().test_cp()

    def test_wc(self):
        super().test_wc()

    def test_diff(self):
        super().test_diff()

    def test_grep(self):
        super().test_grep()
    
    # Functional

    def test_foreach(self):
        super().test_foreach()

    def test_filter(self):
        super().test_filter()

    def test_filter_to(self):
        super().test_filter_to()
