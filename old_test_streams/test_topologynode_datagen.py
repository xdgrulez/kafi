from old_test_streams.test_topologynode_base import TestTopologyNodeBase
from old_test_streams.test_datagen_base import TestDatagenBase

#

class TestTopologyNodeDatagen(TestTopologyNodeBase, TestDatagenBase):
    def test_datagen_1_join(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        root_topologyNode = self.get_topology_1_join(click_source_str, customer_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(click_source_str, 100), (customer_source_str, 100)], 50, root_topologyNode)

    def test_datagen_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        root_topologyNode = self.get_topology_2_joins(click_source_str, customer_source_str, product_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(click_source_str, 100), (customer_source_str, 100), (product_source_str, 100)], 50, root_topologyNode)

    def test_datagen_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        root_topologyNode = self.get_topology_3_joins(click_source_str, customer_source_str, product_source_str, order_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(click_source_str, 100), (customer_source_str, 100), (product_source_str, 100), (order_source_str, 100)], 50, root_topologyNode)
