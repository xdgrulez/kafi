import unittest

import cloudpickle as pickle

#

class TestTopologyNodeBase(unittest.TestCase):
    def process(self, source_str_batch_size_int_tuple_list, steps_int, root_tn):
        for source_str, _ in source_str_batch_size_int_tuple_list:
            self.init_generate(source_str)
        #
        for step_int in range(steps_int):
            for source_str, batch_size_int in source_str_batch_size_int_tuple_list:
                record_any_list = self.generate(source_str, batch_size_int)
                #
                root_tn.push(source_str, record_any_list)
            #
            updated_record_any_list = root_tn.latest()
            self.updated_record_any_list += updated_record_any_list
            self.deleted_record_any_list += []
            #
            print()
            print(f"{step_int + 1}/{steps_int}")
            print(len(pickle.dumps(root_tn)) / 1024)
