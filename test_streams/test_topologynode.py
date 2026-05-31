from test_streams.test_topologynode_base import TestTopologyNodeBase
from test_streams.test_base import TestBase

#

class TestTopologyNode(TestTopologyNodeBase, TestBase):
    def test_datagen_1_join(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        runner = self.get_datagen_1_join_root_tn(click_source_str, customer_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(click_source_str, 100), (customer_source_str, 100)], 100, runner)

    def test_datagen_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        runner = self.get_datagen_2_joins_root_tn(click_source_str, customer_source_str, product_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(click_source_str, 100), (customer_source_str, 100), (product_source_str, 100)], 100, runner)

    def test_datagen_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        runner = self.get_datagen_3_joins_root_tn(click_source_str, customer_source_str, product_source_str, order_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(click_source_str, 100), (customer_source_str, 100), (product_source_str, 100), (order_source_str, 100)], 100, runner)

    #

    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        root_tn = self.get_jamie_root_tn(transaction_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(transaction_source_str, 100)], 100, root_tn)
        #
        self.assertEqual(len(self.updated_message_dict_list), 1)
        self.assertEqual(self.updated_message_dict_list[0]["value"], {"sum": 0})

    #

    def test_wc(self):
        plain_text_source_str = "plain_text"
        #
        root_tn = self.get_wc_root_tn(plain_text_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(plain_text_source_str, 100)], 100, root_tn)
