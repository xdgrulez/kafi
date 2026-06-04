from test_kafi_streams.test_topologynode_base import TestTopologyNodeBase
from test_kafi_streams.test_generate import TestGenerate
from test_kafi_streams.test_base import TestBase, default_batch_size_int, default_steps_int

from test_kafi_streams.datagen.topologies import get_root_tn_datagen_1_join, get_root_tn_datagen_2_joins, get_root_tn_datagen_3_joins, get_root_tn_datagen_self_join_group_by
from test_kafi_streams.jamie.topologies import get_root_tn_jamie
from test_kafi_streams.wc.topologies import get_root_tn_wc

#

class TestTopologyNode(TestTopologyNodeBase, TestGenerate, TestBase):
    def test_datagen_1_join(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        root_tn = get_root_tn_datagen_1_join(click_source_str, customer_source_str)
        #
        self.process([(click_source_str, default_batch_size_int), (customer_source_str, default_batch_size_int)], default_steps_int, root_tn)

    def test_datagen_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        root_tn = get_root_tn_datagen_2_joins(click_source_str, customer_source_str, product_source_str)
        #
        self.process([(click_source_str, default_batch_size_int), (customer_source_str, default_batch_size_int), (product_source_str, default_batch_size_int)], default_steps_int, root_tn)

    def test_datagen_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        root_tn = get_root_tn_datagen_3_joins(click_source_str, customer_source_str, product_source_str, order_source_str)
        #
        self.process([(click_source_str, default_batch_size_int), (customer_source_str, default_batch_size_int), (product_source_str, default_batch_size_int), (order_source_str, default_batch_size_int)], default_steps_int, root_tn)

    def test_datagen_self_join_group_by(self):
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        root_tn = get_root_tn_datagen_self_join_group_by(customer_source_str, product_source_str, order_source_str)
        #
        self.process([(customer_source_str, 5), (product_source_str, 5), (order_source_str, 5)], 2, root_tn)

    #

    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        root_tn = get_root_tn_jamie(transaction_source_str)
        #
        self.process([(transaction_source_str, default_batch_size_int)], default_steps_int, root_tn)
        #
        self.assert_jamie()

    #

    def test_wc(self):
        line_source_str = "lines"
        #
        root_tn = get_root_tn_wc(line_source_str)
        #
        self.process([(line_source_str, default_batch_size_int)], default_steps_int, root_tn)
        #
        self.assert_wc(line_source_str)
