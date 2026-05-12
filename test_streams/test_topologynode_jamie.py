from test_streams.test_topologynode_base import TestTopologyNodeBase
from test_streams.test_jamie_base import TestJamieBase

from pydbsp import (
    DeltaLiftedDeltaLiftedSortMergeJoin,
    Program2D,
    LiftedLiftedProject,
    LiftedLiftedGroupBySum,
    DeltaLiftedDistinct
)

#

class TestTopologyNodeJamie(TestTopologyNodeBase, TestJamieBase):
    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        runner = self.get_runner(transaction_source_str)
        #
        self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(transaction_source_str, 100)], 100, runner)
        #
        # p = Program2D(gc=True)
        # source_topologyNode = root_topologyNode.get_node_by_name(transaction_source_str)
        # p._sources = [source_topologyNode]
        # p.view("root", root_topologyNode.output_stream2D_function())
        # self.source_str_messages_int_dict, self.updated_message_dict_list, self.deleted_message_dict_list = self.process([(transaction_source_str, 100)], 100, root_topologyNode, program2D, view)

        # self.assertEqual(len(self.updated_message_dict_list), 1)
        # self.assertEqual(self.updated_message_dict_list[0]["value"], {"sum": 0})
