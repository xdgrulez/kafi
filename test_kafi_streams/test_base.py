from collections import defaultdict

import json, unittest

#

default_pack_function = json.dumps
default_unpack_function = json.loads

#

default_batch_size_int = 100
default_steps_int = 50

#

class TestBase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print("Test:", self._testMethodName)
        #
        self.source_str_generator_dict = {}
        self.source_str_input_value_any_list_dict = defaultdict(list)
        self.updated_value_any_list = []
        self.deleted_value_any_list = []

    def tearDown(self):
        print()
        print("---")
        print()
        #
        source_str_values_int_dict = {source_str: len(value_any_list) for source_str, value_any_list in self.source_str_input_value_any_list_dict.items()}
        print(f"Inputs: {source_str_values_int_dict}")
        #
        print()
        print("---")
        print()
        #
        self.print_changes(self.updated_value_any_list, "Updates")
        #
        print()
        print("---")
        print()
        #
        self.print_changes(self.deleted_value_any_list, "Deletes")
        #
        print()
        print("---")
        print()

    def print_changes(self, changed_value_any_list, changes_str, select_function=lambda x: x["value"]):
        changed_packed_value_any_list = [default_pack_function(select_function(value_any)) for value_any in changed_value_any_list]
        changes_int = len(changed_packed_value_any_list)
        unique_changes_int = len(set(changed_packed_value_any_list))
        print(f"{changes_str}: {changes_int}")
        print()
        print(f"Unique: {unique_changes_int}")
        print()
        if changes_int > 6:
            print("First three:")
            for changed_packed_value_any in changed_packed_value_any_list[:3]: 
                print(default_unpack_function(changed_packed_value_any))
            print()
            print("Last three:")
            for changed_packed_value_any in changed_packed_value_any_list[-3:]: 
                print(default_unpack_function(changed_packed_value_any))
        elif changes_int > 0:
            print("All:")
            for changed_packed_value_any in changed_packed_value_any_list:
                print(default_unpack_function(changed_packed_value_any))

    #

    def assert_jamie(self):
        self.assertEqual(len(self.updated_value_any_list), 1)
        self.assertEqual(self.updated_value_any_list[0]["value"]["total"], 0)

    #

    def assert_wc(self, line_source_str):
        input_word_str_count_int_dict = {}
        for message_dict in self.source_str_input_value_any_list_dict[line_source_str]:
            line_str = message_dict["value"]
            word_str_list = line_str.split()
            for word_str in word_str_list:
                input_word_str_count_int_dict[word_str] = input_word_str_count_int_dict.get(word_str, 0) + 1
        #
        output_word_str_count_int_dict = {}
        for message_dict in self.updated_value_any_list:
            word_str = message_dict["value"]["word"]
            count_int = message_dict["value"]["count"]
            output_word_str_count_int_dict[word_str] = count_int
        #
        self.assertEqual(input_word_str_count_int_dict, output_word_str_count_int_dict)
