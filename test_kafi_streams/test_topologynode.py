from test_kafi_streams.test_topologynode_base import TestTopologyNodeBase
from test_kafi_streams.test_topologies import TestTopologies
from test_kafi_streams.test_generate import TestGenerate
from test_kafi_streams.test_base import TestBase

#

class TestTopologyNode(TestTopologyNodeBase, TestTopologies, TestGenerate, TestBase):
    def test_datagen_1_join(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        root_tn = self.get_datagen_1_join_root_tn(click_source_str, customer_source_str)
        #
        self.source_str_values_int_dict, self.updated_value_any_list, self.deleted_value_any_list = self.process([(click_source_str, 100), (customer_source_str, 100)], 50, root_tn)

    def test_datagen_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        root_tn = self.get_datagen_2_joins_root_tn(click_source_str, customer_source_str, product_source_str)
        #
        self.source_str_values_int_dict, self.updated_value_any_list, self.deleted_value_any_list = self.process([(click_source_str, 100), (customer_source_str, 100), (product_source_str, 100)], 50, root_tn)

    def test_datagen_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        root_tn = self.get_datagen_3_joins_root_tn(click_source_str, customer_source_str, product_source_str, order_source_str)
        #
        self.source_str_values_int_dict, self.updated_value_any_list, self.deleted_value_any_list = self.process([(click_source_str, 100), (customer_source_str, 100), (product_source_str, 100), (order_source_str, 100)], 50, root_tn)

    #

    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        root_tn = self.get_jamie_root_tn(transaction_source_str)
        #
        self.source_str_values_int_dict, self.updated_value_any_list, self.deleted_value_any_list = self.process([(transaction_source_str, 100)], 50, root_tn)
        #
        self.assertEqual(len(self.updated_value_any_list), 1)
        self.assertEqual(self.updated_value_any_list[0], {"value": {"sum": 0}})

    #

    def test_wc(self):
        plain_text_source_str = "lines"
        #
        root_tn = self.get_wc_root_tn(plain_text_source_str)
        #
        self.source_str_values_int_dict, self.updated_value_any_list, self.deleted_value_any_list = self.process([(plain_text_source_str, 100)], 50, root_tn)
