from test_streams.test_topologynode_base import TestTopologyNodeBase
from test_streams.test_topologynode_streams_base import TestTopologyNodeStreamsBase

#

class TestTopologyNode(TestTopologyNodeBase, TestTopologyNodeStreamsBase):
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
