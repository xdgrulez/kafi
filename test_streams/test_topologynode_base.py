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
        print(f"Inputs: {self.source_str_messages_int_dict}")
        #
        print()
        print("---")
        print()
        #
        updates_int = len(self.updated_message_dict_list)
        updated_message_json_str_list = [json.dumps(message_dict) for message_dict in self.updated_message_dict_list]
        unique_updates_int = len(set(updated_message_json_str_list))
        print(f"Updates: {updates_int}")
        print(f"Unique updates: {unique_updates_int}")
        if updates_int > 0:
            print("First update:")
            print(json.dumps(self.updated_message_dict_list[0], indent=2))
        #
        print()
        print("---")
        print()
        #
        deletes_int = len(self.deleted_message_dict_list)
        print(f"Deletes: {deletes_int}")
        if deletes_int > 0:
            print("First delete:")
            print(json.dumps(self.updated_message_dict_list[0], indent=2))
        #
        print()
        print("---")
        print()

    #

    def produce(self, source_topologyNode, message_dict_list):
        zSet = message_dict_list_to_ZSet(message_dict_list)
        source_topologyNode.output_handle_function().get().send(zSet)
    
    #

    def process(self, source_str_batch_size_int_tuple_list, steps_int, root_topologyNode):
        source_str_messages_int_dict = {source_str: 0 for source_str, _ in source_str_batch_size_int_tuple_list}
        coll_updated_output_dict_list = []
        coll_deleted_output_dict_list = []
        for step_int in range(steps_int):
            for source_str, batch_size_int in source_str_batch_size_int_tuple_list:
                message_dict_list = self.generate(source_str, batch_size_int)
                #
                source_topologyNode = root_topologyNode.get_node_by_name(source_str)
                self.produce(source_topologyNode, message_dict_list)
                source_str_messages_int_dict[source_str] += len(message_dict_list)
            #
            root_topologyNode.step()
            #
            latest_zSet = root_topologyNode.latest()
            updated_output_dict_list, deleted_output_dict_list = zSet_to_message_dict_list_tuple(latest_zSet)
            coll_updated_output_dict_list += updated_output_dict_list
            coll_deleted_output_dict_list += deleted_output_dict_list
            #
            print()
            print(f"{step_int}/{steps_int}")
            # print(f"{step_int} - Latest: {root_topologyNode.latest()}")
            print(len(pickle.dumps(root_topologyNode)) / 1024)
        #
        return source_str_messages_int_dict, coll_updated_output_dict_list, coll_deleted_output_dict_list
