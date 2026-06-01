import json, unittest

#

default_pack_function = json.dumps
default_unpack_function = json.loads

#

class TestBase(unittest.IsolatedAsyncioTestCase):
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

    def print_changes(self, changed_value_any_list, changes_str):
        changed_serialized_value_any_list = [default_pack_function(value_any) for value_any in changed_value_any_list]
        changes_int = len(changed_serialized_value_any_list)
        unique_changes_int = len(set(changed_serialized_value_any_list))
        print(f"{changes_str}: {changes_int}")
        print()
        print(f"Unique: {unique_changes_int}")
        print()
        if changes_int > 6:
            print("First three:")
            for changed_serialized_value_any in changed_serialized_value_any_list[:3]: 
                print(default_unpack_function(changed_serialized_value_any))
            print()
            print("Last three:")
            for changed_serialized_value_any in changed_serialized_value_any_list[-3:]: 
                print(default_unpack_function(changed_serialized_value_any))
        elif changes_int > 0:
            print("Last:")
            print(default_unpack_function(changed_serialized_value_any_list[-1]))
