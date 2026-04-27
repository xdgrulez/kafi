from test.test_topologynode_jamie_base import TestTopologyNodeJamieBase
from kafi.kafi import *

#

class TestTopologyNodeJamie(TestTopologyNodeJamieBase):
    def test_jamie(self):
        root_topologyNode = self.get_topology()
        #
        source_topologyNode = root_topologyNode.get_node_by_name("transactions")
        #
        self.updated_message_dict_list, self.deleted_message_dict_list = self.process(source_topologyNode, root_topologyNode, 1, 10)
