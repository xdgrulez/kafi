from streams.test_topologynode_base import TestTopologyNodeBase
from streams.test_generate import TestGenerate
from streams.test_base import TestBase, default_batch_size_int, default_steps_int

from streams.datagen.topologies import get_built_tn_datagen_1_join, get_built_tn_datagen_2_joins, get_built_tn_datagen_3_joins, get_built_tn_datagen_self_join_group_by, get_built_tn_datagen_self_join_group_by_debezium, get_built_tn_datagen_multiple_sinks, get_built_tn_datagen_expire
from streams.jamie.topologies import get_built_tn_jamie
from streams.wc.topologies import get_built_tn_wc

#

from kafi.streams.topologynode import TopologyNode as Tn

#

class TestTopologyNode(TestTopologyNodeBase, TestGenerate, TestBase):
    def test_datagen_1_join(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        sink_str = "1_join"
        #
        built_tn = get_built_tn_datagen_1_join(lambda: Tn.source(click_source_str),
                                              lambda: Tn.source(customer_source_str),
                                              lambda x: x.sink(sink_str))
        #
        self.process(built_tn,
                     {click_source_str: default_batch_size_int, customer_source_str: default_batch_size_int},
                     default_steps_int)

    def test_datagen_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        sink_str = "2_joins"
        #
        built_tn = get_built_tn_datagen_2_joins(lambda: Tn.source(click_source_str),
                                                lambda: Tn.source(customer_source_str),
                                                lambda: Tn.source(product_source_str),
                                                lambda x: x.sink(sink_str))
        #
        self.process(built_tn,
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
        built_tn = get_built_tn_datagen_3_joins(lambda: Tn.source(click_source_str),
                                               lambda: Tn.source(customer_source_str),
                                               lambda: Tn.source(product_source_str),
                                               lambda: Tn.source(order_source_str),
                                               lambda x: x.sink(sink_str))
        #
        self.process(built_tn,
                     {click_source_str: default_batch_size_int, customer_source_str: default_batch_size_int, product_source_str: default_batch_size_int, order_source_str: default_batch_size_int},
                     default_steps_int)

    def test_datagen_self_join_group_by(self):
        source_str = "shoe_orders"
        #
        sink_str = "self_join_group_by"
        #
        built_tn = get_built_tn_datagen_self_join_group_by(lambda: Tn.source(source_str), lambda x: x.sink(sink_str))
        #
        self.process(built_tn, {source_str: default_batch_size_int}, default_steps_int)
        #
        self.assert_datagen_self_join_group_by(source_str, sink_str)

    def test_datagen_self_join_group_by_debezium(self):
        source_str = "shoe_orders_debezium"
        #
        sink_str = "self_join_group_by_debezium"
        #
        built_tn = get_built_tn_datagen_self_join_group_by_debezium(lambda: Tn.source(source_str), lambda x: x.sink(sink_str))
        #
        self.process(built_tn, {source_str: default_batch_size_int}, default_steps_int)
        #
        self.assert_datagen_self_join_group_by_debezium(source_str, sink_str)

    def test_datagen_multiple_sinks(self):
        source_str = "shoe_customers"
        #
        customer_a_h_str = "customer_a_h"
        customer_i_q_str = "customer_i_q"
        customer_r_z_str = "customer_r_z"
        #
        built_tn = get_built_tn_datagen_multiple_sinks(lambda: Tn.source(source_str), 
                                                       lambda x: x.sink(customer_a_h_str),
                                                       lambda x: x.sink(customer_i_q_str),
                                                       lambda x: x.sink(customer_r_z_str))
        #
        self.process(built_tn, {source_str: default_batch_size_int}, default_steps_int)
        #
        self.assert_datagen_multiple_sinks(customer_a_h_str, customer_i_q_str, customer_r_z_str)

    def test_datagen_expire(self):
        source_str = "shoe_orders"
        #
        sink_str = "shoe_orders_expire"
        #
        built_tn = get_built_tn_datagen_expire(lambda: Tn.source(source_str), lambda x: x.sink(sink_str))
        #
        self.process(built_tn, {source_str: default_batch_size_int}, default_steps_int)
        #
        self.assert_datagen_expire()

    #

    def test_jamie(self):
        source_str = "transactions"
        #
        sink_str = "total"
        #
        built_tn = get_built_tn_jamie(lambda: Tn.source(source_str), lambda x: x.sink(sink_str))
        #
        self.process(built_tn, {source_str: default_batch_size_int}, default_steps_int)
        #
        self.assert_jamie(sink_str)

    #

    def test_wc(self):
        source_str = "lines"
        #
        sink_str = "wc"
        #
        built_tn = get_built_tn_wc(lambda: Tn.source(source_str), lambda x: x.sink(sink_str))
        #
        self.process(built_tn, {source_str: default_batch_size_int}, default_steps_int)
        #
        self.assert_wc(source_str, sink_str)
