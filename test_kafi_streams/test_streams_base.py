import threading, time, unittest

from kafi.helpers import get_millis
from kafi.streams.streams import run_streams

from kafi.streams.topologynode import default_pack_function, default_unpack_function

#

class TestStreamsBase(unittest.IsolatedAsyncioTestCase):
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
        # print()
        # print("---")
        # print()
        # #
        # self.print_changes(self.deleted_value_any_list, "Deletes")
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

    #

    def produce(self, storage_topic_str_batch_size_int_tuple_list, steps_int, key_type, value_type):
        for _ in range(steps_int):
            for storage, topic_str, batch_size_int in storage_topic_str_batch_size_int_tuple_list:
                    message_dict_list = self.generate(topic_str, batch_size_int)
                    #
                    producer = storage.producer(topic_str, key_type=key_type, value_type=value_type)
                    producer.produce_list(message_dict_list)
                    producer.close()
                    #
                    self.source_str_messages_int_dict[topic_str] += batch_size_int

    #

    def process(self, source_storage_topic_str_tuple_list, target_storage, target_topic_str, runner, group_str, key_type, value_type):
        self.stop_function = run_streams(source_storage_topic_str_tuple_list, runner, target_storage, target_topic_str, group=group_str, key_type=key_type, value_type=value_type)

    #

    def read(self, storage, topic_str, key_type, value_type):
        message_dict_list = storage.cat(topic_str, key_type=key_type, value_type=value_type)
        #
        self.source_str_values_int_dict[topic_str] = len(message_dict_list)
        #
        self.updated_value_any_list = message_dict_list


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
    
    def go(self, runner, source_storage_topic_str_batch_size_int_tuple_list, steps_int, target_storage, target_topic_str, key_type="str", value_type="json"):
        group_str = f"test_group_{get_millis()}"
        #
        source_storage_topic_str_tuple_list = [(storage, topic_str) for storage, topic_str, _ in source_storage_topic_str_batch_size_int_tuple_list]
        #
        self.source_str_messages_int_dict = {source_str: 0 for _, source_str in source_storage_topic_str_tuple_list}
        #
        for storage, topic_str in source_storage_topic_str_tuple_list:
            storage.recreate(topic_str)
        target_storage.recreate(target_topic_str)
        #
        for _, topic_str, _ in source_storage_topic_str_batch_size_int_tuple_list:
            self.init_generate(topic_str)
        #
        thread1 = threading.Thread(target=self.produce, args=(source_storage_topic_str_batch_size_int_tuple_list, steps_int, key_type, value_type))
        #
        thread2 = threading.Thread(target=self.process, args=(source_storage_topic_str_tuple_list, target_storage, target_topic_str, runner, group_str, key_type, value_type))
        #
        thread1.start()
        thread2.start()
        #
        while True:
            if all(self.stop(storage, topic_str, batch_size_int, group_str, steps_int) for storage, topic_str, batch_size_int in source_storage_topic_str_batch_size_int_tuple_list):
                break
            #
            time.sleep(0.1)
        #
        self.stop_function()
        #
        thread1.join()
        thread2.join()
        #
        self.read(target_storage, target_topic_str, key_type=key_type, value_type=value_type)
