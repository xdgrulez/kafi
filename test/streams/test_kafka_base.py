from collections import defaultdict
import time

from kafi.helpers import copy_kwargs, get_millis

#

class TestKafkaBase:
    def produce(self, storage_topic_str_batch_size_int_tuple_list, steps_int, **kwargs):
        topic_str_messages_int_dict = defaultdict(int)
        for _ in range(steps_int):
            for storage, topic_str, batch_size_int in storage_topic_str_batch_size_int_tuple_list:
                    message_dict_list = self.generate(topic_str, batch_size_int)
                    #
                    source_kwargs = copy_kwargs(topic_str, **kwargs)
                    producer = storage.producer(topic_str, **source_kwargs)
                    producer.produce_list(message_dict_list)
                    #
                    messages_int = len(message_dict_list)
                    topic_str_messages_int_dict[topic_str] += messages_int
                    print(f"Produced {messages_int} messages to topic {topic_str} ({topic_str_messages_int_dict[topic_str]}/{steps_int * batch_size_int}).")
                    #
                    producer.close()
                    #
                    time.sleep(0.1)

    def read_topics(self, storage_topic_str_tuple_list, source_or_sink_str, **kwargs):
        source_str_input_record_any_list_dict = defaultdict(list)
        for storage, topic_str in storage_topic_str_tuple_list:
            print(f"Reading {source_or_sink_str} topic {topic_str}...")
            source_or_sink_kwargs = copy_kwargs(f"{source_or_sink_str}_{topic_str}", **kwargs)
            source_or_sink_kwargs["group"] = f"test_{get_millis()}"
            message_dict_list = storage.cat(topic_str, **source_or_sink_kwargs)
            source_str_input_record_any_list_dict[topic_str] = message_dict_list
            print("...done.")
        #
        return source_str_input_record_any_list_dict

    def read_source_topics(self, storage_topic_str_tuple_list, **kwargs):
         return self.read_topics(storage_topic_str_tuple_list, "source", **kwargs)
    
    def read_sink_topics(self, storage_topic_str_tuple_list, **kwargs):
         return self.read_topics(storage_topic_str_tuple_list, "sink", **kwargs)