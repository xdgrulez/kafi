from test_streams.test_topologynode_base import TestTopologyNodeBase
from test_streams.test_wc_base import TestWCBase

#

class TestTopologyNodeWC(TestTopologyNodeBase, TestWCBase):
    def test_wc(self):
        plain_text_source_str = "plain_text"
        #
        root_tn = self.get_root_tn(plain_text_source_str)
        # root_topologyNode = runner._root_topologyNode
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(plain_text_source_str, 1)], 5, root_tn)
        #
        for x in self.updated_message_dict_list:
            print(x)
        # self.assertEqual(len(self.updated_message_dict_list), 1)
        # self.assertEqual(self.updated_message_dict_list[0]["value"], {"sum": 0})
        #
        # runner1 = Runner(root_topologyNode)
        # self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(transaction_source_str, 100)], 100, runner1)
        
        # self.assertEqual(len(self.updated_message_dict_list), 0)
