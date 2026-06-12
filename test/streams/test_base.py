from collections import defaultdict

import json, unittest

#

default_pack_function = json.dumps
default_unpack_function = json.loads

#

default_batch_size_int = 100
default_steps_int = 20

#

class TestBase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print("Test:", self._testMethodName)
        #
        self.source_str_generator_dict = {}
        self.source_str_input_record_any_list_dict = defaultdict(list)
        self.updated_record_any_list = []
        self.deleted_record_any_list = []

    def tearDown(self):
        print()
        print("---")
        print()
        #
        source_str_values_int_dict = {source_str: len(record_any_list) for source_str, record_any_list in self.source_str_input_record_any_list_dict.items()}
        print(f"Inputs: {source_str_values_int_dict}")
        #
        print()
        print("---")
        print()
        #
        self.print_changes(self.updated_record_any_list, "Updates")
        #
        print()
        print("---")
        print()
        #
        self.print_changes(self.deleted_record_any_list, "Deletes")
        #
        print()
        print("---")
        print()

    def print_changes(self, changed_record_any_list, changes_str, select_function=lambda x: x["value"]):
        changed_packed_record_any_list = [default_pack_function(select_function(record_any)) for record_any in changed_record_any_list]
        changes_int = len(changed_packed_record_any_list)
        unique_changes_int = len(set(changed_packed_record_any_list))
        print(f"{changes_str}: {changes_int}")
        print()
        print(f"Unique: {unique_changes_int}")
        print()
        if changes_int > 6:
            print("First three:")
            for changed_packed_record_any in changed_packed_record_any_list[:3]: 
                print(default_unpack_function(changed_packed_record_any))
            print()
            print("Last three:")
            for changed_packed_record_any in changed_packed_record_any_list[-3:]: 
                print(default_unpack_function(changed_packed_record_any))
        elif changes_int > 0:
            print("All:")
            for changed_packed_record_any in changed_packed_record_any_list:
                print(default_unpack_function(changed_packed_record_any))

    def assert_self_join_group_by(self, order_source_str):
        print("Asserting self_join_group_by...")
        #
        message_dict_list = self.source_str_input_record_any_list_dict[order_source_str]
        product_id_str_product_id_str_tuple_customer_id_set_dict = defaultdict(set)
        #
        for message_dict1 in message_dict_list:
            value_dict1 = message_dict1["value"]
            for message_dict2 in message_dict_list:
                value_dict2 = message_dict2["value"]
                if value_dict1["product_id"] < value_dict2["product_id"] and value_dict1["customer_id"] == value_dict2["customer_id"]:
                    product_id_str_product_id_str_tuple_customer_id_set_dict[(value_dict1["product_id"], value_dict2["product_id"])].add(value_dict1["customer_id"])
        #
        json_str_set1 = set([json.dumps({"product_id_1": product_id_str_product_id_str_tuple[0], "product_id_2": product_id_str_product_id_str_tuple[1], "cross_purchases": len(customer_id_set)}) for product_id_str_product_id_str_tuple, customer_id_set in product_id_str_product_id_str_tuple_customer_id_set_dict.items()])
        #
        json_str_set2 = set([json.dumps(message_dict["value"]) for message_dict in self.updated_record_any_list])
        self.assertTrue(json_str_set1.issubset(json_str_set2))
    
    def assert_self_join_group_by_debezium(self, order_source_str):
        print("Asserting self_join_group_by_debezium...")
        #
        message_dict_list = self.source_str_input_record_any_list_dict[order_source_str]
        created_value_dict_list = [message_dict["value"]["after"] for message_dict in message_dict_list if message_dict["value"]["op"] == "c"]
        deleted_value_dict_list = [message_dict["value"]["before"] for message_dict in message_dict_list if message_dict["value"]["op"] == "d"]
        diff_value_dict_list = [value_dict for value_dict in created_value_dict_list if value_dict not in deleted_value_dict_list]
        #
        product_id_str_product_id_str_tuple_customer_id_set_dict = defaultdict(set)
        #
        for value_dict1 in diff_value_dict_list:
            for value_dict2 in diff_value_dict_list:
                if value_dict1["product_id"] < value_dict2["product_id"] and value_dict1["customer_id"] == value_dict2["customer_id"]:
                    product_id_str_product_id_str_tuple_customer_id_set_dict[(value_dict1["product_id"], value_dict2["product_id"])].add(value_dict1["customer_id"])
        #
        json_str_set1 = set([json.dumps({"product_id_1": product_id_str_product_id_str_tuple[0], "product_id_2": product_id_str_product_id_str_tuple[1], "cross_purchases": len(customer_id_set)}) for product_id_str_product_id_str_tuple, customer_id_set in product_id_str_product_id_str_tuple_customer_id_set_dict.items()])
        #
        created_value_dict_list1 = [message_dict["value"]["after"] for message_dict in self.updated_record_any_list if message_dict["value"]["op"] == "c"]
        deleted_value_dict_list1 = [message_dict["value"]["before"] for message_dict in self.updated_record_any_list if message_dict["value"]["op"] == "d"]
        diff_value_dict_list1 = [value_dict for value_dict in created_value_dict_list1 if value_dict not in deleted_value_dict_list1]
        json_str_set2 = set([json.dumps(value_dict) for value_dict in diff_value_dict_list1])
        #
        self.assertTrue(json_str_set2.issubset(json_str_set1))
        #
        print("...done.")

    #

    def assert_jamie(self):
        self.assertEqual(len(self.updated_record_any_list), 1)
        self.assertEqual(self.updated_record_any_list[0]["value"]["total"], 0)

    #

    def assert_wc(self, line_source_str):
        input_word_str_count_int_dict = {}
        for message_dict in self.source_str_input_record_any_list_dict[line_source_str]:
            line_str = message_dict["value"]
            word_str_list = line_str.split()
            for word_str in word_str_list:
                input_word_str_count_int_dict[word_str] = input_word_str_count_int_dict.get(word_str, 0) + 1
        #
        output_word_str_count_int_dict = {}
        for message_dict in self.updated_record_any_list:
            word_str = message_dict["value"]["word"]
            count_int = message_dict["value"]["count"]
            output_word_str_count_int_dict[word_str] = count_int
        #
        self.assertEqual(input_word_str_count_int_dict, output_word_str_count_int_dict)
