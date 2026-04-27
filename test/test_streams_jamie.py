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
        source_storage_topic_str_tuple_list = [(source_storage, source_topic_str)]
        #
        target_storage = source_storage
        target_topic_str = "total"
        #
        steps_int = 100
        batch_size_int = 100
        #
        self.go(root_topologyNode, source_storage_topic_str_tuple_list, target_storage, target_topic_str, steps_int, batch_size_int)
