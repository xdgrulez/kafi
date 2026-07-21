from streams.test_streams_base import TestStreamsBase
from streams.test_generate import TestGenerate
from streams.test_base import TestBase, default_batch_size_int, default_steps_int

from streams.datagen.topologies import get_built_tn_datagen_1_join, get_built_tn_datagen_2_joins, get_built_tn_datagen_3_joins, get_built_tn_datagen_self_join_group_by, get_built_tn_datagen_self_join_group_by_debezium, get_built_tn_datagen_multiple_sinks, get_built_tn_datagen_tumbling_window, get_built_tn_datagen_hopping_window, get_built_tn_datagen_cumulative_window, get_built_tn_datagen_sliding_window, get_built_tn_datagen_session_window
from streams.jamie.topologies import get_built_tn_jamie
from streams.wc.topologies import get_built_tn_wc

#

from kafi.kafi import Cluster
from kafi.streams.streams import Streams

import cloudpickle

#

class TestStreams(TestStreamsBase, TestGenerate, TestBase):
    def test_datagen_1_join(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        sink_str = "1_join"
        #
        source_storage = Cluster("local")
        source_storage.consume_batch_size(default_batch_size_int)
        sink_storage = source_storage
        #
        built_tn = get_built_tn_datagen_1_join(lambda: Streams.source(source_storage, click_source_str),
                                              lambda: Streams.source(source_storage, customer_source_str),
                                              lambda x: x.sink(sink_storage, sink_str))
        #
        self.go(built_tn,
                {click_source_str: default_batch_size_int, customer_source_str: default_batch_size_int},
                default_steps_int)

    def test_datagen_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        sink_str = "2_joins"
        #
        source_storage = Cluster("local")
        source_storage.consume_batch_size(default_batch_size_int)
        sink_storage = source_storage
        #
        built_tn = get_built_tn_datagen_2_joins(lambda: Streams.source(source_storage, click_source_str),
                                               lambda: Streams.source(source_storage, customer_source_str),
                                               lambda: Streams.source(source_storage, product_source_str),
                                               lambda x: x.sink(sink_storage, sink_str))
        #
        self.go(built_tn,
                {click_source_str: default_batch_size_int, customer_source_str: default_batch_size_int, product_source_str: default_batch_size_int},
                default_steps_int)

    def test_datagen_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        sink_str = "3_joins"
        #
        source_storage = Cluster("local")
        source_storage.consume_batch_size(default_batch_size_int)
        sink_storage = source_storage
        #
        built_tn = get_built_tn_datagen_3_joins(lambda: Streams.source(source_storage, click_source_str),
                                                lambda: Streams.source(source_storage, customer_source_str),
                                                lambda: Streams.source(source_storage, product_source_str),
                                                lambda: Streams.source(source_storage, order_source_str),
                                                lambda x: x.sink(sink_storage, sink_str))
        #
        self.go(built_tn,
                {click_source_str: default_batch_size_int, customer_source_str: default_batch_size_int, product_source_str: default_batch_size_int, order_source_str: default_batch_size_int},
                default_steps_int)

    def test_datagen_self_join_group_by(self):
        source_str = "shoe_orders"
        sink_str = "self_join_group_by"
        #
        source_storage = Cluster("local")
        source_storage.consume_batch_size(default_batch_size_int)
        sink_storage = source_storage
        #
        built_tn = get_built_tn_datagen_self_join_group_by(lambda: Streams.source(source_storage, source_str),
                                                           lambda x: x.sink(sink_storage, sink_str))
        #
        self.go(built_tn, {source_str: default_batch_size_int}, default_steps_int)
        #
        self.assert_datagen_self_join_group_by(source_str, sink_str)

    def test_datagen_self_join_group_by_debezium(self):
        source_str = "shoe_orders_debezium"
        sink_str = "self_join_group_by_debezium"
        #
        source_storage = Cluster("local")
        source_storage.consume_batch_size(default_batch_size_int)
        sink_storage = source_storage
        #
        built_tn = get_built_tn_datagen_self_join_group_by_debezium(lambda: Streams.source(source_storage, source_str),
                                                                    lambda x: x.sink(sink_storage, sink_str))
        #
        self.go(built_tn, {source_str: default_batch_size_int}, default_steps_int)
        #
        self.assert_datagen_self_join_group_by_debezium(source_str, sink_str)

    def test_datagen_multiple_sinks(self):
        source_str = "shoe_customers"
        #
        sink_customer_a_h_str = "customer_a_h"
        sink_customer_i_q_str = "customer_i_q"
        sink_customer_r_z_str = "customer_r_z"
        #
        source_storage = Cluster("local")
        source_storage.consume_batch_size(default_batch_size_int)
        sink_storage = source_storage
        #
        built_tn = get_built_tn_datagen_multiple_sinks(lambda: Streams.source(source_storage, source_str),
                                                       lambda x: x.sink(sink_storage, sink_customer_a_h_str),
                                                       lambda x: x.sink(sink_storage, sink_customer_i_q_str),
                                                       lambda x: x.sink(sink_storage, sink_customer_r_z_str))
        #
        source_str_batch_size_int_dict = {source_str: default_batch_size_int}
        #
        self.go(built_tn, source_str_batch_size_int_dict, default_steps_int)
        #
        self.assert_datagen_multiple_sinks(sink_customer_a_h_str, sink_customer_i_q_str, sink_customer_r_z_str)

    #

    def _test_datagen_window(self, sink_str, get_built_tn_fun):
        # old_recursion_limit_int = sys.getrecursionlimit()
        # sys.setrecursionlimit(10000)
        #
        order_source_str = "shoe_orders"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        sink_str = "tumbling_window"
        #
        source_storage = Cluster("local")
        source_storage.consume_batch_size(default_batch_size_int)
        sink_storage = source_storage
        #
        built_tn = get_built_tn_fun(lambda: Streams.source(source_storage, order_source_str),
                                    lambda: Streams.source(source_storage, customer_source_str),
                                    lambda: Streams.source(source_storage, product_source_str),
                                    lambda x: x.sink(sink_storage, sink_str))
        #
        source_str_batch_size_int_dict = {order_source_str: default_batch_size_int,
                                          customer_source_str: default_batch_size_int,
                                          product_source_str: default_batch_size_int}
        #
        self.go(built_tn, source_str_batch_size_int_dict, default_steps_int)
        #
        # sys.setrecursionlimit(old_recursion_limit_int)

    def test_datagen_tumbling_window(self):
        sink_str = "tumbling_window"
        #
        self._test_datagen_window(sink_str, get_built_tn_datagen_tumbling_window)

    def test_datagen_hopping_window(self):
        sink_str = "hopping_window"
        #
        self._test_datagen_window(sink_str, get_built_tn_datagen_hopping_window)

    def test_datagen_cumulative_window(self):
        sink_str = "cumulative_window"
        #
        self._test_datagen_window(sink_str, get_built_tn_datagen_cumulative_window)
    
    def test_datagen_sliding_window(self):
        sink_str = "sliding_window"
        #
        self._test_datagen_window(sink_str, get_built_tn_datagen_sliding_window)

    def test_datagen_session_window(self):
        sink_str = "session_window"
        #
        self._test_datagen_window(sink_str, get_built_tn_datagen_session_window)

    #

    def test_jamie(self):
        source_str = "transactions"
        sink_str = "total"
        #
        source_storage = Cluster("local")
        source_storage.consume_batch_size(default_batch_size_int)
        sink_storage = source_storage
        #
        source_topic_str = source_str + "_topic"
        sink_topic_str = sink_str + "_topic"
        #
        built_tn = get_built_tn_jamie(lambda: Streams.source(source_storage, source_str, source_topic_str),
                                      lambda x: x.sink(sink_storage, sink_str, sink_topic_str))
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
        source_storage.consume_batch_size(default_batch_size_int)
        sink_storage = source_storage
        #
        source_topic_str = source_str
        sink_topic_str = sink_str
        #
        built_tn = get_built_tn_wc(lambda: Streams.source(source_storage, source_str, source_topic_str, value_type="str"),
                                   lambda x: x.sink(sink_storage, sink_str, sink_topic_str))
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
