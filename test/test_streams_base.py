import asyncio, json, random, threading, time, unittest

from kafi.helpers import get_millis
from kafi.streams.streams import run_streams

#

class TestStreamsBase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        print("Test:", self._testMethodName)

    async def asyncTearDown(self):
        print()
        print("---")
        print()
        #
        print("Updates:")
        for message_dict in self.updated_message_dict_list:
            print(json.dumps(message_dict, indent=2))
        #
        print()
        print("---")
        print()
        #
        print("Deletes:")
        # for message_dict in self.deleted_message_dict_list:
        #     print(json.dumps(message_dict, indent=2))
        #
        print()
        print("---")
        print()
        #
        print(f"Number of updates: {len(self.updated_message_dict_list)}")
        # print(f"Number of deletes: {len(self.deleted_message_dict_list)}")

    #

    def produce(self, storage_topic_str_tuple_list, steps_int, batch_size_int):
        for _ in range(steps_int):
            for storage, topic_str in storage_topic_str_tuple_list:
                message_dict_list = self.generate(topic_str, batch_size_int)
                #
                producer = storage.producer(topic_str)
                producer.produce_list(message_dict_list)
                producer.close()

    #

    def process(self, source_storage_topic_str_tuple_list, target_storage, target_topic_str, root_topologyNode, group_str):
        self.stop_function = run_streams(source_storage_topic_str_tuple_list, root_topologyNode, target_storage, target_topic_str, group=group_str)

    #

    def read(self, storage, topic_str):
        message_dict_list = storage.cat(topic_str)
        #
        self.updated_message_dict_list = [message_dict["value"] for message_dict in message_dict_list]

    #

    def stop(self, source_storage, source_topic_str, group_str, steps_int, batch_size_int):
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
    
    def go(self, root_topologyNode, source_storage_topic_str_tuple_list, target_storage, target_topic_str, steps_int, batch_size_int):
        group_str = f"test_group_{get_millis()}"
        #
        for storage, topic_str in source_storage_topic_str_tuple_list:
            storage.recreate(topic_str)
        target_storage.recreate(target_topic_str)
        #
        thread1 = threading.Thread(target=self.produce, args=(source_storage_topic_str_tuple_list, steps_int, batch_size_int))
        thread2 = threading.Thread(target=self.process, args=(source_storage_topic_str_tuple_list, target_storage, target_topic_str, root_topologyNode, group_str))
        #
        thread1.start()
        thread2.start()
        #
        while True:
            if all(self.stop(storage, topic_str, group_str, steps_int, batch_size_int) for storage, topic_str in source_storage_topic_str_tuple_list):
                break
            #
            time.sleep(0.1)
        #
        self.stop_function()
        #
        thread1.join()
        thread2.join()
        #
        self.read(target_storage, target_topic_str)
