from streams.test_streams_base import TestStreamsBase
from streams.test_generate import TestGenerate
from streams.test_base import TestBase, default_batch_size_int, default_steps_int

from streams.datagen.topologies import get_root_tn_datagen_1_join, get_root_tn_datagen_2_joins, get_root_tn_datagen_3_joins, get_root_tn_datagen_self_join_group_by, get_root_tn_datagen_self_join_group_by_debezium
from streams.jamie.topologies import get_root_tn_jamie
from streams.wc.topologies import get_root_tn_wc

#

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
        sink_storage = source_storage
        sink_topic_str = "1_join"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, sink_storage, sink_topic_str)

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
        sink_storage = source_storage
        sink_topic_str = "2_joins"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, sink_storage, sink_topic_str)

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
        sink_storage = source_storage
        sink_topic_str = "3_joins"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, sink_storage, sink_topic_str)

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
        sink_storage = source_storage
        sink_topic_str = "self_join_group_by"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, sink_storage, sink_topic_str)
        #
        self.assert_self_join_group_by(order_source_str)

    def test_datagen_self_join_group_by_debezium(self):
        order_source_str = "shoe_orders_debezium"
        #
        root_tn = get_root_tn_datagen_self_join_group_by_debezium(order_source_str)
        #
        source_storage = Cluster("local")
        #
        order_topic_str = order_source_str
        #
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, order_topic_str, default_batch_size_int)]
        #
        sink_storage = source_storage
        sink_topic_str = "self_join_group_by_debezium"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, sink_storage, sink_topic_str)
        #
        self.assert_self_join_group_by_debezium(order_source_str)

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
        sink_storage = source_storage
        sink_topic_str = "total"
        #
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, sink_storage, sink_topic_str)
        #
        self.assert_jamie()

    #

    def test_wc(self, recreate_boolean=True):
        line_source_str = "lines"
        #
        root_tn = get_root_tn_wc(line_source_str)
        #
        source_storage = Cluster("local")
        #
        transaction_topic_str = line_source_str
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, transaction_topic_str, default_batch_size_int)]
        #
        sink_storage = source_storage
        sink_topic_str = "wc"
        #
        snapshot_storage = source_storage
        snapshot_topic_str = "wc_snapshot"
        #
        kwargs = {f"{line_source_str}_value_type": "str"}
        self.go(root_tn, source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, sink_storage, sink_topic_str, snapshot_storage, snapshot_topic_str, recreate_boolean=recreate_boolean, **kwargs)
        #
        self.assert_wc(line_source_str)

    def test_wc_crash(self):
        self.test_wc(False)
