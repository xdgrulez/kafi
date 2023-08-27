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

    ### ClusterAdmin
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
        super().test_delete_groups()

    def test_group_offsets(self):
        super().test_group_offsets()

    def test_set_group_offsets(self):
        super().test_set_group_offsets()

    # Topics

    def test_config_set_config(self):
        super().test_config_set_config()

    def test_create_delete(self):
        super().test_create_delete()

    def test_topics(self):
        super().test_topics()

    def test_offsets_for_times(self):
        super().test_offsets_for_times()

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
        super().test_consume_from_offsets()

    def test_commit(self):
        super().test_commit()
    
    def test_error_handling(self):
        super().test_error_handling()

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
