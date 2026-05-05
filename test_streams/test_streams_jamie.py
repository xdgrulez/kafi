import os, sys

#

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test_streams.test_streams_base import TestStreamsBase
from test_streams.test_jamie_base import TestJamieBase

from kafi.kafi import Cluster

#

class TestStreamsJamie(TestStreamsBase, TestJamieBase):
    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        root_topologyNode = self.get_topology(transaction_source_str)
        #
        source_storage = Cluster("local")
        #
        transaction_topic_str = transaction_source_str
        source_storage_topic_str_batch_size_int_tuple_list = [(source_storage, transaction_topic_str, 100)]
        #
        target_storage = source_storage
        target_topic_str = "total"
        #
        self.go(root_topologyNode, source_storage_topic_str_batch_size_int_tuple_list, 100, target_storage, target_topic_str)
        #
        self.assertEqual(len(self.updated_message_dict_list), 1)
        self.assertEqual(self.updated_message_dict_list[0]["value"], {"sum": 0})
