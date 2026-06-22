from streams.test_streams_base import TestStreamsBase
from streams.test_generate import TestGenerate
from streams.test_base import TestBase, default_batch_size_int, default_steps_int

from streams.datagen.topologies import get_root_tn_datagen_1_join, get_root_tn_datagen_2_joins, get_root_tn_datagen_3_joins, get_root_tn_datagen_self_join_group_by, get_root_tn_datagen_self_join_group_by_debezium
from streams.jamie.topologies import get_built_tn_jamie
from streams.wc.topologies import get_built_tn_wc

#

from kafi.kafi import Cluster
from kafi.streams.streams import Streams

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
        sink_root_tn_storage_topic_str_tuple_list = [(root_tn, sink_storage, sink_topic_str)]
        #
        self.go(source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, sink_root_tn_storage_topic_str_tuple_list)

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
        sink_root_tn_storage_topic_str_tuple_list = [(root_tn, sink_storage, sink_topic_str)]
        #
        self.go(source_storage_topic_str_batch_size_int_tuple_list, default_steps_int, sink_root_tn_storage_topic_str_tuple_list)

    def test_datagen_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        sink_str = "3_joins"
        #
        root_tn = get_root_tn_datagen_3_joins(click_source_str, customer_source_str, product_source_str, order_source_str, sink_str)
        #
        source_storage = Cluster("local")
        sink_storage = source_storage
        #
        source_str_topic_dict_or_storage_batch_size_int_tuple_list = [
            (click_source_str, source_storage, default_batch_size_int),
            (customer_source_str, source_storage, default_batch_size_int),
            (product_source_str, source_storage, default_batch_size_int),
            (order_source_str, source_storage, default_batch_size_int)
            ]
        #
        sink_storage = source_storage
        sink_str_topic_dict_or_storage_dict = {sink_str: sink_storage}
        #
        self.go(source_str_topic_dict_or_storage_batch_size_int_tuple_list, default_steps_int, root_tn, sink_str_topic_dict_or_storage_dict)

    def test_datagen_self_join_group_by(self):
        order_source_str = "shoe_orders"
        sink_str = "self_join_group_by"
        #
        root_tn = get_root_tn_datagen_self_join_group_by(order_source_str, sink_str)
        #
        source_storage = Cluster("local")
        sink_storage = source_storage
        #
        order_topic_str = order_source_str
        source_str_topic_dict_batch_size_int_tuple_list = [(order_source_str,
                                                            {"storage": source_storage,
                                                             "topic": order_topic_str},
                                                             default_batch_size_int)]
        #
        sink_topic_str = sink_str
        sink_str_topic_dict_dict = {sink_str: {"storage": sink_storage,
                                               "topic": sink_topic_str}}
        #
        self.go(source_str_topic_dict_batch_size_int_tuple_list, default_steps_int, root_tn, sink_str_topic_dict_dict)
        #
        self.assert_self_join_group_by(order_source_str, sink_str)

    def test_datagen_self_join_group_by_debezium(self):
        order_source_str = "shoe_orders_debezium"
        sink_str = "self_join_group_by_debezium"
        #
        root_tn = get_root_tn_datagen_self_join_group_by_debezium(order_source_str, sink_str)
        #
        source_storage = Cluster("local")
        sink_storage = source_storage
        #
        order_topic_str = order_source_str
        source_str_topic_dict_batch_size_int_tuple_list = [(order_source_str,
                                                            {"storage": source_storage,
                                                             "topic": order_topic_str},
                                                             default_batch_size_int)]
        #
        sink_topic_str = sink_str
        sink_str_topic_dict_dict = {sink_str: {"storage": sink_storage,
                                               "topic": sink_topic_str}}
        #
        self.go(source_str_topic_dict_batch_size_int_tuple_list, default_steps_int, root_tn, sink_str_topic_dict_dict)
        #
        self.assert_self_join_group_by_debezium(order_source_str, sink_str)

    #

    def test_jamie(self):
        source_str = "transactions"
        sink_str = "total"
        #
        source_storage = Cluster("local")
        sink_storage = source_storage
        #
        source_topic_str = source_str + "_topic"
        sink_topic_str = sink_str + "_topic"
        #
        built_tn = get_built_tn_jamie(lambda: Streams.source(source_str, source_storage, source_topic_str),
                                      lambda x: x.sink(sink_str, sink_storage, sink_topic_str))
        #
        source_str_batch_size_int_dict = {source_str: default_batch_size_int}
        #
        self.go(built_tn, source_str_batch_size_int_dict, default_steps_int)
        #
        self.assert_jamie(sink_str)

    #

    def test_wc(self, recreate_boolean=True):
        source_str = "lines"
        sink_str = "wc"
        #
        source_storage = Cluster("local")
        sink_storage = source_storage
        #
        source_topic_str = source_str
        sink_topic_str = sink_str
        #
        built_tn = get_built_tn_wc(lambda: Streams.source(source_str, source_storage, source_topic_str, value_type="str"),
                                   lambda x: x.sink(sink_str, sink_storage, sink_topic_str))
        #
        checkpoint_storage = source_storage
        checkpoint_topic_str = "wc_checkpoint"
        #
        source_str_batch_size_int_dict = {source_str: default_batch_size_int}
        #
        self.go(built_tn, source_str_batch_size_int_dict, default_steps_int, checkpoint_storage, checkpoint_topic_str, recreate_boolean=recreate_boolean)
        #
        self.assert_wc(source_str, sink_topic_str)

    def test_wc_crash(self):
        self.test_wc(False)
