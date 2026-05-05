import os, sys

#

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test_streams.test_streams_base import TestStreamsBase
from test_streams.test_datagen_base import TestDatagenBase

from kafi.kafi import Cluster

#

class TestStreamsDatagen(TestStreamsBase, TestDatagenBase):
    def test_datagen_1_join(self):
        root_topologyNode = self.get_topology()
        #
        source_storage = Cluster("local")
        #
        click_topic_str = "shoe_clickstream"
        customer_topic_str = "shoe_customers"
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, 100), (source_storage, customer_topic_str, 100)]
        #
        target_storage = source_storage
        target_topic_str = "1_join"
        #
        self.go(root_topologyNode, source_storage_topic_str_batch_size_int_tuple_list, 100, target_storage, target_topic_str)
