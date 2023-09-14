import os
import sys

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from test.test_single_storage_base import TestSingleStorageBase
from kafi.storage import *
from kafi.helpers import *

#

class TestSingleFSBase(TestSingleStorageBase):
    def test_filter_file_to_topic(self):
        if self.__class__.__name__ == "TestSingleFSBase":
            return
        #
        s = self.get_storage()
        #
        file_str = "./snacks.txt"
        producer = s.producer(file_str, value_type="json")
        producer.produce(self.snack_str_list)
        producer.close()
        #
        topic_str2 = self.create_test_topic_name()
        #
        group_str1 = self.create_test_group_name()
        (consume_n_int, written_n_int) = s.filter_to(file_str, s, topic_str2, group=group_str1, filter_function=lambda message_dict: message_dict["value"]["calories"] > 100, source_type="json", target_type="json")
        self.assertEqual(3, consume_n_int)
        self.assertEqual(2, written_n_int)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list, n_int) = s.cat(topic_str2, group=group_str2, type="json", n=2)
        self.assertEqual(2, len(message_dict_list))
        self.assertEqual(2, n_int)
        self.assertEqual(500.0, message_dict_list[0]["value"]["calories"])
        self.assertEqual(260.0, message_dict_list[1]["value"]["calories"])

    # def test_parquet(self):
    #     if self.__class__.__name__ == "TestSingleFSBase":
    #         return
    #     #
    #     s = self.get_storage()
    #     #
    #     topic_str1 = self.create_test_topic_name()
    #     s.create(topic_str1)
    #     producer = s.producer(topic_str1, value_type="json")
    #     producer.produce(self.snack_str_list)
    #     producer.close()
    #     #
    #     group_str1 = self.create_test_group_name()
    #     (consume_n_int, written_n_int) = s.cp(topic_str1, s, "./snacks.parquet", group=group_str1, source_type="json")
    #     self.assertEqual(3, consume_n_int)
    #     self.assertEqual(3, written_n_int)
    #     #
    #     (message_dict_list, n_int) = s.cat("./snacks.parquet")
    #     self.assertEqual(3, len(message_dict_list))
    #     self.assertEqual(3, n_int)
    #     value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
    #     self.assertEqual(value_dict_list, self.snack_dict_list)
