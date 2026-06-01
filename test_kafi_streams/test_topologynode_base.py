import unittest

import cloudpickle as pickle

from kafi.streams.topologynode import default_pack_function, default_unpack_function

#

class TestTopologyNodeBase(unittest.TestCase):
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
