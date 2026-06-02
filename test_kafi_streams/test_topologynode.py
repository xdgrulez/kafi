from test_kafi_streams.test_topologynode_base import TestTopologyNodeBase
from test_kafi_streams.test_generate import TestGenerate
from test_kafi_streams.test_base import TestBase

from test_kafi_streams.datagen.topologies import get_datagen_1_join_root_tn, get_datagen_2_joins_root_tn, get_datagen_3_joins_root_tn
from test_kafi_streams.jamie.topologies import get_jamie_root_tn
from test_kafi_streams.wc.topologies import get_wc_root_tn

#

class TestTopologyNode(TestTopologyNodeBase, TestGenerate, TestBase):
    def test_datagen_1_join(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        #
        root_tn = get_datagen_1_join_root_tn(click_source_str, customer_source_str)
        #
        self.process([(click_source_str, 100), (customer_source_str, 100)], 50, root_tn)

    def test_datagen_2_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        #
        root_tn = get_datagen_2_joins_root_tn(click_source_str, customer_source_str, product_source_str)
        #
        self.process([(click_source_str, 100), (customer_source_str, 100), (product_source_str, 100)], 50, root_tn)

    def test_datagen_3_joins(self):
        click_source_str = "shoe_clickstream"
        customer_source_str = "shoe_customers"
        product_source_str = "shoes"
        order_source_str = "shoe_orders"
        #
        root_tn = get_datagen_3_joins_root_tn(click_source_str, customer_source_str, product_source_str, order_source_str)
        #
        self.process([(click_source_str, 100), (customer_source_str, 100), (product_source_str, 100), (order_source_str, 100)], 50, root_tn)

    #

    def test_jamie(self):
        transaction_source_str = "transactions"
        #
        root_tn = get_jamie_root_tn(transaction_source_str)
        #
        self.process([(transaction_source_str, 100)], 50, root_tn)
        #
        self.assertEqual(len(self.updated_value_any_list), 1)
        self.assertEqual(self.updated_value_any_list[0], {"value": {"total": 0}})

    #

    def test_wc(self):
        plain_text_source_str = "lines"
        #
        root_tn = get_wc_root_tn(plain_text_source_str)
        #
        self.process([(plain_text_source_str, 100)], 50, root_tn)
        #
        input_word_str_count_int_dict = {}
        for message_dict in self.source_str_input_value_any_list_dict[plain_text_source_str]:
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
