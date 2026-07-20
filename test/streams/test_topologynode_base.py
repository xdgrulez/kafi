import unittest

import cloudpickle as pickle

#

class TestTopologyNodeBase(unittest.TestCase):
    def process(self, built_tn, source_str_batch_size_int_dict, steps_int):
        for source_str, _ in source_str_batch_size_int_dict.items():
            self.init_generate(source_str)
        #
        for step_int in range(steps_int):
            for source_str, batch_size_int in source_str_batch_size_int_dict.items():
                r_list = self.generate(source_str, batch_size_int)
                self.source_str_input_r_list_dict[source_str] += r_list
                #
                built_tn.push(source_str, r_list)
            #
            sink_str_output_any_dict = built_tn.latest()
            for sink_str in built_tn._sink_str_list:
                self.sink_str_updated_r_list_dict[sink_str] += sink_str_output_any_dict.get(sink_str, [])
            #
            print()
            print(f"{step_int + 1}/{steps_int}")
            kbytes_int = len(pickle.dumps(built_tn)) / 1024
            self.step_int_kbytes_int_dict[step_int] = kbytes_int
            print(kbytes_int)
