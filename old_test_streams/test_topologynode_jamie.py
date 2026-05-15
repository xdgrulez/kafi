from old_test_streams.test_topologynode_base import TestTopologyNodeBase
from old_test_streams.test_jamie_base import TestJamieBase

#

class TestTopologyNodeJamie(TestTopologyNodeBase, TestJamieBase):
    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        root_topologyNode = self.get_topology(transaction_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(transaction_source_str, 100)], 100, root_topologyNode)
        #
        self.assertEqual(len(self.updated_message_dict_list), 1)
        self.assertEqual(self.updated_message_dict_list[0]["value"], {"sum": 0})
