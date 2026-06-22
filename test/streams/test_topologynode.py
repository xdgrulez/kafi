from streams.test_topologynode_base import TestTopologyNodeBase
from streams.test_generate import TestGenerate
from streams.test_base import TestBase, default_batch_size_int, default_steps_int

from streams.datagen.topologies import get_root_tn_datagen_1_join, get_root_tn_datagen_2_joins, get_root_tn_datagen_3_joins, get_root_tn_datagen_self_join_group_by, get_root_tn_datagen_self_join_group_by_debezium, get_root_tn_datagen_multiple_sinks
from streams.jamie.topologies import get_built_tn_jamie
from streams.wc.topologies import get_root_tn_wc

#

class TestTopologyNode(TestTopologyNodeBase, TestGenerate, TestBase):
    def test_datagen_1_join(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        join_1_sink_str = "1_join"
        #
        root_tn = get_root_tn_datagen_1_join(click_source_str, customer_source_str, join_1_sink_str)
        #
        self.process([(click_source_str, default_batch_size_int), (customer_source_str, default_batch_size_int)], default_steps_int, root_tn, [join_1_sink_str])

    def test_datagen_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        joins_2_sink_str = "2_joins"
        #
        root_tn = get_root_tn_datagen_2_joins(click_source_str, customer_source_str, product_source_str, joins_2_sink_str)
        #
        self.process([(click_source_str, default_batch_size_int), (customer_source_str, default_batch_size_int), (product_source_str, default_batch_size_int)], default_steps_int, root_tn, [joins_2_sink_str])

    def test_datagen_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        joins_3_sink_str = "3_joins"
        #
        root_tn = get_root_tn_datagen_3_joins(click_source_str, customer_source_str, product_source_str, order_source_str, joins_3_sink_str)
        #
        self.process([(click_source_str, default_batch_size_int), (customer_source_str, default_batch_size_int), (product_source_str, default_batch_size_int), (order_source_str, default_batch_size_int)], default_steps_int, root_tn, [joins_3_sink_str])

    def test_datagen_self_join_group_by(self):
        order_source_str = "shoe_orders"
        #
        self_join_group_by_sink_str = "self_join_group_by"
        #
        root_tn = get_root_tn_datagen_self_join_group_by(order_source_str, self_join_group_by_sink_str)
        #
        self.process([(order_source_str, default_batch_size_int)], default_steps_int, root_tn, [self_join_group_by_sink_str])
        #
        self.assert_self_join_group_by(order_source_str, self_join_group_by_sink_str)

    def test_datagen_self_join_group_by_debezium(self):
        order_source_str = "shoe_orders_debezium"
        #
        self_join_group_by_debezium_sink_str = "self_join_group_by_debezium"
        #
        root_tn = get_root_tn_datagen_self_join_group_by_debezium(order_source_str, self_join_group_by_debezium_sink_str)
        #
        self.process([(order_source_str, default_batch_size_int)], default_steps_int, root_tn, [self_join_group_by_debezium_sink_str])
        #
        self.assert_self_join_group_by_debezium(order_source_str, self_join_group_by_debezium_sink_str)

    def test_datagen_multiple_sinks(self):
        customer_source_str = "shoe_customers"
        #
        customer_a_h_str = "customer_a_h"
        customer_i_q_str = "customer_i_q"
        customer_r_z_str = "customer_r_z"
        #
        root_tn = get_root_tn_datagen_multiple_sinks(customer_source_str, customer_a_h_str, customer_i_q_str, customer_r_z_str)
        #
        self.process([(customer_source_str, default_batch_size_int)], default_steps_int, root_tn, [customer_a_h_str, customer_i_q_str, customer_r_z_str])

    #

    def test_jamie(self):
        source_str = "transactions"
        #
        sink_str = "total"
        #
        built_tn = get_built_tn_jamie(source_str, sink_str)
        #
        self.process(built_tn, {source_str: default_batch_size_int}, default_steps_int)
        #
        self.assert_jamie(sink_str)

    #

    def test_wc(self):
        line_source_str = "lines"
        #
        wc_sink_str = "wc"
        #
        root_tn = get_root_tn_wc(line_source_str, wc_sink_str)
        #
        self.process([(line_source_str, default_batch_size_int)], default_steps_int, root_tn, [wc_sink_str])
        #
        self.assert_wc(line_source_str, wc_sink_str)
