from collections import defaultdict

import json, sys, unittest

#

default_pack_fun = json.dumps
default_unpack_fun = json.loads

#

default_batch_size_int = 100
default_steps_int = 20

#

class TestBase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print("Test:", self._testMethodName)
        #
        self.source_str_generator_dict = {}
        #
        self.source_str_input_r_list_dict = defaultdict(list)
        self.sink_str_updated_r_list_dict = defaultdict(list)
        #
        self.step_int_kbytes_int_dict = {}

    def tearDown(self):
        print()
        print("---")
        print()
        #
        source_str_values_int_dict = {source_str: len(r_list) for source_str, r_list in self.source_str_input_r_list_dict.items()}
        print(f"Inputs: {source_str_values_int_dict}")
        #
        print()
        print("---")
        print()
        #
        for sink_str, updated_r_list in self.sink_str_updated_r_list_dict.items():
            self.print_changes(updated_r_list, f"Updates for sink \"{sink_str}\"")
            print()
            print("-")
            print()

    def print_changes(self, changed_r_list, changes_str, select_fun=lambda x: x["value"]):
        changed_packed_r_list = [default_pack_fun(select_fun(r)) for r in changed_r_list]
        changes_int = len(changed_packed_r_list)
        unique_changes_int = len(set(changed_packed_r_list))
        print(f"{changes_str}: {changes_int}")
        print()
        print(f"Unique: {unique_changes_int}")
        print()
        if changes_int > 6:
            print("First three:")
            for changed_packed_r in changed_packed_r_list[:3]: 
                print(default_unpack_fun(changed_packed_r))
            print()
            print("Last three:")
            for changed_packed_r in changed_packed_r_list[-3:]: 
                print(default_unpack_fun(changed_packed_r))
        elif changes_int > 0:
            print("All:")
            for changed_packed_r in changed_packed_r_list:
                print(default_unpack_fun(changed_packed_r))

    #

    def assert_datagen_self_join_group_by(self, order_source_str, self_join_group_by_sink_str):
        function_str = sys._getframe().f_code.co_name
        print(f"Asserting {function_str}...")
        #
        m_list = self.source_str_input_r_list_dict[order_source_str]
        product_id_str_product_id_str_tuple_customer_id_set_dict = defaultdict(set)
        #
        for m1 in m_list:
            value_dict1 = m1["value"]
            for m2 in m_list:
                value_dict2 = m2["value"]
                if value_dict1["product_id"] < value_dict2["product_id"] and value_dict1["customer_id"] == value_dict2["customer_id"]:
                    product_id_str_product_id_str_tuple_customer_id_set_dict[(value_dict1["product_id"], value_dict2["product_id"])].add(value_dict1["customer_id"])
        #
        json_str_set1 = set([json.dumps({"product_id_1": product_id_str_product_id_str_tuple[0], "product_id_2": product_id_str_product_id_str_tuple[1], "cross_purchases": len(customer_id_set)}) for product_id_str_product_id_str_tuple, customer_id_set in product_id_str_product_id_str_tuple_customer_id_set_dict.items()])
        #
        updated_r_list = self.sink_str_updated_r_list_dict[self_join_group_by_sink_str]
        json_str_set2 = set([json.dumps(m["value"]) for m in updated_r_list])
        self.assertTrue(json_str_set1.issubset(json_str_set2))
    
    def assert_datagen_self_join_group_by_debezium(self, order_source_str, self_join_group_by_debezium_sink_str):
        function_str = sys._getframe().f_code.co_name
        print(f"Asserting {function_str}...")
        #
        m_list = self.source_str_input_r_list_dict[order_source_str]
        created_value_dict_list = [m["value"]["after"] for m in m_list if m["value"]["op"] == "c"]
        deleted_value_dict_list = [m["value"]["before"] for m in m_list if m["value"]["op"] == "d"]
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
        updated_r_list = self.sink_str_updated_r_list_dict[self_join_group_by_debezium_sink_str]
        created_value_dict_list1 = [m["value"]["after"] for m in updated_r_list if m["value"]["op"] == "c"]
        deleted_value_dict_list1 = [m["value"]["before"] for m in updated_r_list if m["value"]["op"] == "d"]
        diff_value_dict_list1 = [value_dict for value_dict in created_value_dict_list1 if value_dict not in deleted_value_dict_list1]
        json_str_set2 = set([json.dumps(value_dict) for value_dict in diff_value_dict_list1])
        #
        self.assertTrue(json_str_set2.issubset(json_str_set1))
        #
        print("...done.")

    def assert_datagen_multiple_sinks(self, sink_customer_a_h_str, sink_customer_i_q_str, sink_customer_r_z_str):
        function_str = sys._getframe().f_code.co_name
        print(f"Asserting {function_str}...")
        #
        for m in self.sink_str_updated_r_list_dict[sink_customer_a_h_str]:
            self.assertTrue(m["value"]["last_name"][0].lower() >= "A".lower() and m["value"]["last_name"][0].lower() <= "H".lower())
        #
        for m in self.sink_str_updated_r_list_dict[sink_customer_i_q_str]:
            self.assertTrue(m["value"]["last_name"][0].lower() >= "I".lower() and m["value"]["last_name"][0].lower() <= "Q".lower())
        #
        for m in self.sink_str_updated_r_list_dict[sink_customer_r_z_str]:
            self.assertTrue(m["value"]["last_name"][0].lower() >= "R".lower() and m["value"]["last_name"][0].lower() <= "Z".lower())
        #
        print("...done.")

    def assert_datagen_expire(self):
        function_str = sys._getframe().f_code.co_name
        print(f"Asserting {function_str}...")
        #
        steps_int = max(self.step_int_kbytes_int_dict.keys())
        self.assertEqual(self.step_int_kbytes_int_dict[steps_int - 1], self.step_int_kbytes_int_dict[steps_int])
        #
        print("...done.")

    #

    def assert_jamie(self, sink_str):
        function_str = sys._getframe().f_code.co_name
        print(f"Asserting {function_str}...")
        #
        updated_r_list = self.sink_str_updated_r_list_dict[sink_str]
        self.assertEqual(len(updated_r_list), 1)
        self.assertEqual(updated_r_list[0]["value"]["total"], 0)
        #
        print("...done.")

    #

    def assert_wc(self, line_source_str, sink_str):
        function_str = sys._getframe().f_code.co_name
        print(f"Asserting {function_str}...")
        #
        input_word_str_count_int_dict = {}
        for m in self.source_str_input_r_list_dict[line_source_str]:
            line_str = m["value"]
            word_str_list = line_str.split()
            for word_str in word_str_list:
                input_word_str_count_int_dict[word_str] = input_word_str_count_int_dict.get(word_str, 0) + 1
        #
        output_word_str_count_int_dict = {}
        for m in self.sink_str_updated_r_list_dict[sink_str]:
            word_str = m["value"]["word"]
            count_int = m["value"]["count"]
            output_word_str_count_int_dict[word_str] = count_int
        #
        self.assertEqual(input_word_str_count_int_dict, output_word_str_count_int_dict)
        #
        print("...done.")
