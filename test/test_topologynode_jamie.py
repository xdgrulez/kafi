from test.test_topologynode_jamie_base import TestTopologyNodeJamieBase
from kafi.kafi import *

#

class TestTopologyNodeJamie(TestTopologyNodeJamieBase):
    def test_jamie(self):
        source_topologyNode, root_topologyNode = self.get_topologyNode_tuple()
        #
        self.updated_message_dict_list, self.deleted_message_dict_list = self.process(source_topologyNode, root_topologyNode, 100, 100)
