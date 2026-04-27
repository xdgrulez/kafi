import asyncio, json, random, threading, time, unittest

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
        # for message_dict in self.updated_message_dict_list:
        #     print(json.dumps(message_dict, indent=2))
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
        # print(f"Number of updates: {len(self.updated_message_dict_list)}")
        # print(f"Number of deletes: {len(self.deleted_message_dict_list)}")

    #

    def produce(self, storage_topic_str_tuple_list, steps_int, batch_size_int):
        for _ in range(steps_int):
            for storage, topic_str in storage_topic_str_tuple_list:
                message_dict_list = self.generate(batch_size_int)
                #
                producer = storage.producer(topic_str)
                producer.produce_list(message_dict_list)
                producer.close()

    #

    def process(self, source_storage_topic_str_tuple_list, target_storage, target_topic_str, root_topologyNode):
        self.stop_function = run_streams(source_storage_topic_str_tuple_list, root_topologyNode, target_storage, target_topic_str)

    #

    def read(self, storage, topic_str):
        message_dict_list = storage.cat(topic_str)
        #
        return message_dict_list
