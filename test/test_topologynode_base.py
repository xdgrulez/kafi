import json, unittest

import cloudpickle as pickle

from kafi.streams.topologynode import (
    message_dict_list_to_ZSet,
    zSet_to_message_dict_list_tuple
)

#

class TestTopologyNodeBase(unittest.TestCase):
    def setUp(self):
        print("Test:", self._testMethodName)

    def tearDown(self):
        print()
        print("---")
        print()
        #
        print("Updates:")
        for message_dict in self.updated_message_dict_list:
            print(json.dumps(message_dict, indent=2))
        #
        print()
        print("---")
        print()
        #
        print("Deletes:")
        for message_dict in self.deleted_message_dict_list:
            print(json.dumps(message_dict, indent=2))
        #
        print()
        print("---")
        print()
        #
        print(f"Number of updates: {len(self.updated_message_dict_list)}")
        print(f"Number of deletes: {len(self.deleted_message_dict_list)}")

    #

    def step(self, source_topologyNode, root_topologyNode, message_dict_list):
        zSet = message_dict_list_to_ZSet(message_dict_list)
        source_topologyNode.output_handle_function().get().send(zSet)
        #
        root_topologyNode.step()

    #

    def process(self, source_topologyNode, root_topologyNode, steps_int, batch_size_int):
        coll_updated_output_dict_list = []
        coll_deleted_output_dict_list = []
        for i in range(steps_int):
            message_dict_list = self.generate(batch_size_int)
            #
            self.step(source_topologyNode, root_topologyNode, message_dict_list)
            #
            latest_zSet = root_topologyNode.latest()
            updated_output_dict_list, deleted_output_dict_list = zSet_to_message_dict_list_tuple(latest_zSet)
            coll_updated_output_dict_list += updated_output_dict_list
            coll_deleted_output_dict_list += deleted_output_dict_list
            #
            print()
            print(f"{i} - Latest: {root_topologyNode.latest()}")
            print(len(pickle.dumps(root_topologyNode)) / 1024)
        #
        return coll_updated_output_dict_list, coll_deleted_output_dict_list
