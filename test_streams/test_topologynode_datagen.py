from test_streams.test_topologynode_base import TestTopologyNodeBase
from test_streams.test_datagen_base import TestDatagenBase

#

class TestTopologyNodeDatagen(TestTopologyNodeBase, TestDatagenBase):
    def test_1_join(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        runner = self.get_runner_1_join(click_source_str, customer_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(click_source_str, 100), (customer_source_str, 100)], 100, runner)

    def test_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        runner = self.get_runner_2_joins(click_source_str, customer_source_str, product_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(click_source_str, 100), (customer_source_str, 100), (product_source_str, 100)], 100, runner)

    def test_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        runner = self.get_runner_3_joins(click_source_str, customer_source_str, product_source_str, order_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(click_source_str, 100), (customer_source_str, 100), (product_source_str, 100), (order_source_str, 100)], 100, runner)

    def test_co_purchase(self):
        purchase_source_str = "purchases"
        #
        runner = self.get_runner_co_purchase(purchase_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(purchase_source_str, 1)], 4, runner)
