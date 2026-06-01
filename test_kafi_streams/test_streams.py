import os, sys

#

# if os.path.basename(os.getcwd()) == "test":
#     sys.path.insert(1, "..")
# else:
#     sys.path.insert(1, ".")

from test_kafi_streams.test_streams_base import TestStreamsBase
from test_kafi_streams.test_base import TestBase

from kafi.kafi import Cluster

#

class TestStreams(TestStreamsBase, TestBase):
    def test_datagen_1_join(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        root_tn = self.get_datagen_1_join_root_tn(click_source_str, customer_source_str)
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
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, 50, target_storage, target_topic_str)

    def test_datagen_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        root_tn = self.get_datagen_2_joins_root_tn(click_source_str, customer_source_str, product_source_str)
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
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, 50, target_storage, target_topic_str)

    def test_datagen_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        root_tn = self.get_datagen_3_joins_root_tn(click_source_str, customer_source_str, product_source_str, order_source_str)
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
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, 50, target_storage, target_topic_str)

    #

    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        root_tn = self.get_jamie_root_tn(transaction_source_str)
        #
        source_storage = Cluster("local")
        #
        transaction_topic_str = transaction_source_str
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, transaction_topic_str, 100)]
        #
        target_storage = source_storage
        target_topic_str = "total"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, 100, target_storage, target_topic_str)
        #
        self.assertEqual(len(self.updated_value_any_list), 1)
        self.assertEqual(self.updated_value_any_list[0]["value"], {"sum": 0})

    #

    def test_wc(self):
        line_source_str = "lines"
        #
        root_tn = self.get_wc_root_tn(line_source_str)
        #
        source_storage = Cluster("local")
        #
        transaction_topic_str = line_source_str
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, transaction_topic_str, 100)]
        #
        target_storage = source_storage
        target_topic_str = "total"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, 100, target_storage, target_topic_str, key_type="str", value_type="str")
