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
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        root_topologyNode = self.get_topology_1_join(click_source_str, customer_source_str)
        #
        source_storage = Cluster("local")
        #
        click_topic_str = click_source_str
        customer_topic_str = customer_source_str
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, 100), (source_storage, customer_topic_str, 100)]
        #
        target_storage = source_storage
        target_topic_str = "1_join"
        #
        self.go(root_topologyNode, source_storage_topic_str_batch_size_int_tuple_list, 50, target_storage, target_topic_str)

    def test_datagen_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        root_topologyNode = self.get_topology_2_joins(click_source_str, customer_source_str, product_source_str)
        #
        source_storage = Cluster("local")
        #
        click_topic_str = click_source_str
        customer_topic_str = customer_source_str
        product_topic_str = product_source_str
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, 100), (source_storage, customer_topic_str, 100), (source_storage, product_topic_str, 100)]
        #
        target_storage = source_storage
        target_topic_str = "2_joins"
        #
        self.go(root_topologyNode, source_storage_topic_str_batch_size_int_tuple_list, 50, target_storage, target_topic_str)

    def test_datagen_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        root_topologyNode = self.get_topology_3_joins(click_source_str, customer_source_str, product_source_str, order_source_str)
        #
        source_storage = Cluster("local")
        #
        click_topic_str = click_source_str
        customer_topic_str = customer_source_str
        product_topic_str = product_source_str
        order_topic_str = order_source_str
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, 100), (source_storage, customer_topic_str, 100), (source_storage, product_topic_str, 100), (source_storage, order_topic_str, 100)]
        #
        target_storage = source_storage
        target_topic_str = "3_joins"
        #
        self.go(root_topologyNode, source_storage_topic_str_batch_size_int_tuple_list, 50, target_storage, target_topic_str)
