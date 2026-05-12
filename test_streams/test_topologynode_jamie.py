from test_streams.test_topologynode_base import TestTopologyNodeBase
from test_streams.test_jamie_base import TestJamieBase

from kafi.streams.topologynode import (
    Runner
)

#

class TestTopologyNodeJamie(TestTopologyNodeBase, TestJamieBase):
    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        runner = self.get_runner(transaction_source_str)
        # root_topologyNode = runner._root_topologyNode
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(transaction_source_str, 100)], 100, runner)
        #
        self.assertEqual(len(self.updated_message_dict_list), 1)
        self.assertEqual(self.updated_message_dict_list[0]["value"], {"sum": 0})
        #
        # runner1 = Runner(root_topologyNode)
        # self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(transaction_source_str, 100)], 100, runner1)
        
        # self.assertEqual(len(self.updated_message_dict_list), 0)
