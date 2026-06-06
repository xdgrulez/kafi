import threading, time

from test_kafi_streams.test_kafka_base import TestKafkaBase

from kafi.helpers import get_millis
from kafi.streams.streams import run_streams

#

class TestStreamsBase(TestKafkaBase):
    def process(self, source_storage_topic_str_tuple_list, target_storage, target_topic_str, root_tn, group_str, key_type, value_type):
        self.stop_function = run_streams(source_storage_topic_str_tuple_list, root_tn, target_storage, target_topic_str, group=group_str, key_type=key_type, value_type=value_type)

    #

    def stop(self, source_storage, source_topic_str, batch_size_int, group_str, steps_int):
        group_str_topic_str_offsets_dict_dict_dict = source_storage.group_offsets(group_str)
        if group_str not in group_str_topic_str_offsets_dict_dict_dict:
            return False
        topic_str_offsets_dict_dict = group_str_topic_str_offsets_dict_dict_dict[group_str]
        if source_topic_str not in topic_str_offsets_dict_dict:
            return False
        #
        offsets_dict = topic_str_offsets_dict_dict[source_topic_str]
        offset_int = offsets_dict[0]
        return offset_int >= steps_int * batch_size_int

    #
    
    def go(self, root_tn, source_storage_topic_str_batch_size_int_tuple_list, steps_int, target_storage, target_topic_str, group_str=f"test_group_{get_millis()}", source_key_type="str", source_value_type="json", target_key_type="str", target_value_type="json"):
        #
        source_storage_topic_str_tuple_list = [(storage, topic_str) for storage, topic_str, _ in source_storage_topic_str_batch_size_int_tuple_list]
        #
        self.source_str_values_int_dict = {source_str: 0 for _, source_str in source_storage_topic_str_tuple_list}
        #
        for storage, topic_str in source_storage_topic_str_tuple_list:
            storage.recreate(topic_str)
        target_storage.recreate(target_topic_str)
        #
        for _, topic_str, _ in source_storage_topic_str_batch_size_int_tuple_list:
            self.init_generate(topic_str)
        #
        thread1 = threading.Thread(target=self.produce, args=(source_storage_topic_str_batch_size_int_tuple_list, steps_int, source_key_type, source_value_type))
        #
        thread2 = threading.Thread(target=self.process, args=(source_storage_topic_str_tuple_list, target_storage, target_topic_str, root_tn, group_str, source_key_type, source_value_type))
        #
        thread1.start()
        thread2.start()
        #
        while True:
            if all(self.stop(storage, topic_str, batch_size_int, group_str, steps_int) for storage, topic_str, batch_size_int in source_storage_topic_str_batch_size_int_tuple_list):
                time.sleep(1)
                break
            #
            time.sleep(0.1)
        #
        self.stop_function()
        #
        thread1.join()
        thread2.join()
        #
        self.read_source_topics(source_storage_topic_str_tuple_list, key_type=source_key_type, value_type=source_value_type)
        #
        self.read_target_topic(target_storage, target_topic_str, key_type=target_key_type, value_type=target_value_type)
