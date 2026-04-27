from test.test_topologynode_base import TestTopologyNodeBase
from test.test_jamie_base import TestJamieBase

#

class TestTopologyNodeJamie(TestTopologyNodeBase, TestJamieBase):
    def test_jamie(self):
        root_topologyNode = self.get_topology()
        #
        source_topologyNode = root_topologyNode.get_node_by_name("transactions")
        #
        self.updated_message_dict_list, self.deleted_message_dict_list = self.process(source_topologyNode, root_topologyNode, 100, 100)
