from test_streams.test_topologynode_base import TestTopologyNodeBase
from test_streams.test_datagen_base import TestDatagenBase

#

class TestTopologyNodeDatagen(TestTopologyNodeBase, TestDatagenBase):
    def test_datagen_1_join(self):
        clickstream_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        root_topologyNode = self.get_topology()
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(clickstream_source_str, 100), (customer_source_str, 100)], 100, root_topologyNode)
