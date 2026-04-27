from test.test_topologynode_base import TestTopologyNodeBase
from test.test_jamie_base import TestJamieBase

#

class TestTopologyNodeJamie(TestTopologyNodeBase, TestJamieBase):
    def test_jamie(self):
        source_str = "transactions"
        #
        root_topologyNode = self.get_topology()
        #
        self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(source_str, 100, 100)], root_topologyNode)
