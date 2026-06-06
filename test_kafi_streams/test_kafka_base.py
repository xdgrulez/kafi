from collections import defaultdict
import time

#

class TestKafkaBase:
    def produce(self, storage_topic_str_batch_size_int_tuple_list, steps_int, key_type, value_type):
        topic_str_messages_int_dict = defaultdict(int)
        for _ in range(steps_int):
            for storage, topic_str, batch_size_int in storage_topic_str_batch_size_int_tuple_list:
                    message_dict_list = self.generate(topic_str, batch_size_int)
                    #
                    producer = storage.producer(topic_str, key_type=key_type, value_type=value_type)
                    producer.produce_list(message_dict_list)
                    #
                    messages_int = len(message_dict_list)
                    topic_str_messages_int_dict[topic_str] += messages_int
                    print(f"Produced {messages_int} messages to topic {topic_str} ({topic_str_messages_int_dict[topic_str]}/{steps_int * batch_size_int}).")
                    #
                    producer.close()
                    #
                    time.sleep(0.1)

    def read_source_topics(self, source_storage_topic_str_tuple_list, key_type, value_type):
        for source_storage, topic_str in source_storage_topic_str_tuple_list:
            print(f"Reading source topic {topic_str}...")
            message_dict_list = source_storage.cat(topic_str, key_type=key_type, value_type=value_type)
            self.source_str_input_record_any_list_dict[topic_str] = message_dict_list
            print("...done.")

    def read_target_topic(self, storage, topic_str, key_type, value_type):
        print(f"Reading target topic {topic_str}...")
        message_dict_list = storage.cat(topic_str, key_type=key_type, value_type=value_type)
        print("...done.")
        #
        self.updates_int = len(message_dict_list)
        #
        self.updated_record_any_list = message_dict_list
