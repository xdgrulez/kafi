import unittest

import cloudpickle as pickle

from kafi.streams.topologynode import default_pack_function, default_unpack_function

#

class TestTopologyNodeBase(unittest.TestCase):
    def setUp(self):
        print("Test:", self._testMethodName)
        #
        self.source_str_values_int_dict = {}
        self.updated_value_any_list = {}
        self.deleted_value_any_list = {}
        self.generator_dict = {}

    def tearDown(self):
        print()
        print("---")
        print()
        #
        print(f"Inputs: {self.source_str_values_int_dict}")
        #
        print()
        print("---")
        print()
        #
        updated_serialized_value_any_list = [default_pack_function(value_any) for value_any in self.updated_value_any_list]
        updates_int = len(updated_serialized_value_any_list)
        unique_updates_int = len(set(updated_serialized_value_any_list))
        print(f"Updates: {updates_int}")
        print(f"Unique updates: {unique_updates_int}")
        if updates_int > 0:
            print("Last update:")
            print(default_unpack_function(updated_serialized_value_any_list[-1]))
        #
        print()
        print("---")
        print()
        #
        deleted_serialized_value_any_list = [default_pack_function(value_any) for value_any in self.deleted_value_any_list]
        deletes_int = len(deleted_serialized_value_any_list)
        unique_deletes_int = len(set(deleted_serialized_value_any_list))
        print(f"Deletes: {deletes_int}")
        print(f"Unique deletes: {unique_deletes_int}")
        if deletes_int > 0:
            print("Last update:")
            print(default_unpack_function(deleted_serialized_value_any_list[-1]))
        #
        print()
        print("---")
        print()

    #

    def process(self, source_str_batch_size_int_tuple_list, steps_int, root_tn):
        source_str_source_values_int_dict = {source_str: 0 for source_str, _ in source_str_batch_size_int_tuple_list}
        coll_updated_output_any_list = []
        coll_deleted_output_any_list = []
        for source_str, _ in source_str_batch_size_int_tuple_list:
            self.init_generate(source_str)
        for step_int in range(steps_int):
            for source_str, batch_size_int in source_str_batch_size_int_tuple_list:
                value_any_list = self.generate(source_str, batch_size_int)
                #
                root_tn.push(source_str, value_any_list)
                source_str_source_values_int_dict[source_str] += len(value_any_list)
            #
            updated_output_dict_list, deleted_output_dict_list = root_tn.step(bag=True)
            coll_updated_output_any_list += updated_output_dict_list
            coll_deleted_output_any_list += deleted_output_dict_list
            #
            print()
            print(f"{step_int}/{steps_int}")
            print(len(pickle.dumps(root_tn)) / 1024)
        #
        return source_str_source_values_int_dict, coll_updated_output_any_list, coll_deleted_output_any_list
