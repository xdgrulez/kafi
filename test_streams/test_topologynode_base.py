import json, unittest

import cloudpickle as pickle

#

class TestTopologyNodeBase(unittest.TestCase):
    def setUp(self):
        print("Test:", self._testMethodName)
        #
        self.source_str_messages_int_dict = {}
        self.updated_message_dict_list = {}
        self.deleted_message_dict_list = {}
        self.generator_dict = {}

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

    def process(self, source_str_batch_size_int_tuple_list, steps_int, root_tn):
        source_str_messages_int_dict = {source_str: 0 for source_str, _ in source_str_batch_size_int_tuple_list}
        coll_updated_output_dict_list = []
        coll_deleted_output_dict_list = []
        for source_str, _ in source_str_batch_size_int_tuple_list:
            self.init_generate(source_str)
        for step_int in range(steps_int):
            for source_str, batch_size_int in source_str_batch_size_int_tuple_list:
                message_dict_list = self.generate(source_str, batch_size_int)
                #
                root_tn.push(source_str, message_dict_list)
                source_str_messages_int_dict[source_str] += len(message_dict_list)
            #
            updated_output_dict_list, deleted_output_dict_list = root_tn.step()
            coll_updated_output_dict_list += updated_output_dict_list
            coll_deleted_output_dict_list += deleted_output_dict_list
            #
            print()
            print(f"{step_int}/{steps_int}")
            print(len(pickle.dumps(root_tn)) / 1024)
        #
        return source_str_messages_int_dict, coll_updated_output_dict_list, coll_deleted_output_dict_list
