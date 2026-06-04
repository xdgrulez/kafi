from test_kafi_streams.test_streams_base import TestStreamsBase
from test_kafi_streams.test_generate import TestGenerate
from test_kafi_streams.test_base import TestBase, default_batch_size_int, default_steps_int

from test_kafi_streams.datagen.topologies import get_root_tn_datagen_1_join, get_root_tn_datagen_2_joins, get_root_tn_datagen_3_joins, get_root_tn_datagen_self_join_group_by
from test_kafi_streams.jamie.topologies import get_root_tn_jamie
from test_kafi_streams.wc.topologies import get_root_tn_wc

from kafi.kafi import Cluster

#

class TestStreams(TestStreamsBase, TestGenerate, TestBase):
    def test_datagen_1_join(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        root_tn = get_root_tn_datagen_1_join(click_source_str, customer_source_str)
        #
        source_storage = Cluster("local")
        #
        click_topic_str = click_source_str
        customer_topic_str = customer_source_str
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, default_batch_size_int), (source_storage, customer_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "1_join"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, target_storage, target_topic_str)

    def test_datagen_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        root_tn = get_root_tn_datagen_2_joins(click_source_str, customer_source_str, product_source_str)
        #
        source_storage = Cluster("local")
        #
        click_topic_str = click_source_str
        customer_topic_str = customer_source_str
        product_topic_str = product_source_str
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, default_batch_size_int), (source_storage, customer_topic_str, default_batch_size_int), (source_storage, product_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "2_joins"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, target_storage, target_topic_str)

    def test_datagen_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        root_tn = get_root_tn_datagen_3_joins(click_source_str, customer_source_str, product_source_str, order_source_str)
        #
        source_storage = Cluster("local")
        #
        click_topic_str = click_source_str
        customer_topic_str = customer_source_str
        product_topic_str = product_source_str
        order_topic_str = order_source_str
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, click_topic_str, default_batch_size_int), (source_storage, customer_topic_str, default_batch_size_int), (source_storage, product_topic_str, default_batch_size_int), (source_storage, order_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "3_joins"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, target_storage, target_topic_str)

    def test_datagen_self_join_group_by(self):
        order_source_str = "shoe_orders"
        #
        root_tn = get_root_tn_datagen_self_join_group_by(order_source_str)
        #
        source_storage = Cluster("local")
        #
        order_topic_str = order_source_str
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, order_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "self_join"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, 20, target_storage, target_topic_str)
        #
        self.assert_self_join_group_by(order_source_str)

    #

    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        root_tn = get_root_tn_jamie(transaction_source_str)
        #
        source_storage = Cluster("local")
        #
        transaction_topic_str = transaction_source_str
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, transaction_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "jamie_total"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, target_storage, target_topic_str)
        #
        self.assert_jamie()

    #

    def test_wc(self):
        line_source_str = "lines"
        #
        root_tn = get_root_tn_wc(line_source_str)
        #
        source_storage = Cluster("local")
        #
        transaction_topic_str = line_source_str
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, transaction_topic_str, default_batch_size_int)]
        #
        target_storage = source_storage
        target_topic_str = "wc_total"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, target_storage, target_topic_str, source_value_type="str", target_value_type="json")
        #
        self.assert_wc(line_source_str)
