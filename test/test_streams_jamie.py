import os, sys, threading

#

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test.test_streams_base import TestStreamsBase
from test.test_jamie_base import TestJamieBase

from kafi.kafi import Cluster

#

class TestStreamsJamie(TestStreamsBase, TestJamieBase):
    def test_jamie(self):
        root_topologyNode = self.get_topology()
        #
        source_storage = Cluster("local")
        #
        source_topic_str = "transactions"
        source_storage_topic_str_steps_int_batch_size_int_tuple_list = [(source_storage, source_topic_str, 100, 100)]
        #
        target_storage = source_storage
        target_topic_str = "total"
        #
        self.go(root_topologyNode, source_storage_topic_str_steps_int_batch_size_int_tuple_list, target_storage, target_topic_str)
        #
        self.assertEqual(len(self.updated_message_dict_list), 1)
        self.assertEqual(self.updated_message_dict_list[0]["value"], {"sum": 0})
