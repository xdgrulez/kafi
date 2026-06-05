import unittest

import cloudpickle as pickle

#

class TestTopologyNodeBase(unittest.TestCase):
    def process(self, source_str_batch_size_int_tuple_list, steps_int, root_tn, push_function=None, step_function=None):
        if push_function is None:
            push_function = root_tn.push
        #
        if step_function is None:
            step_function = root_tn.step
        #
        for source_str, _ in source_str_batch_size_int_tuple_list:
            self.init_generate(source_str)
        #
        for step_int in range(steps_int):
            for source_str, batch_size_int in source_str_batch_size_int_tuple_list:
                value_any_list = self.generate(source_str, batch_size_int)
                #
                push_function(source_str, value_any_list)
            #
            updated_value_any_list, deleted_value_any_list = step_function(bag=True)
            self.updated_value_any_list += updated_value_any_list
            self.deleted_value_any_list += deleted_value_any_list
            #
            print()
            print(f"{step_int}/{steps_int}")
            print(len(pickle.dumps(root_tn)) / 1024)
