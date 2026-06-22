from collections import defaultdict
import time

from kafi.helpers import copy_kwargs, get_millis

#

class TestKafkaBase:
    def produce(self, source_str_topic_dict_dict, source_str_batch_size_int_dict, steps_int):
        topic_str_messages_int_dict = defaultdict(int)
        for _ in range(steps_int):
            for source_str, topic_dict in source_str_topic_dict_dict.items():
                    storage = topic_dict["storage"]
                    topic_str = topic_dict["topic"]
                    kwargs = topic_dict.get("kwargs", {})
                    #
                    batch_size_int = source_str_batch_size_int_dict[source_str]
                    #
                    message_dict_list = self.generate(source_str, batch_size_int)
                    #
                    producer = storage.producer(topic_str, **kwargs)
                    producer.produce_list(message_dict_list)
                    #
                    messages_int = len(message_dict_list)
                    topic_str_messages_int_dict[topic_str] += messages_int
                    print(f"Produced {messages_int} messages to topic {topic_str} ({topic_str_messages_int_dict[topic_str]}/{steps_int * batch_size_int}).")
                    #
                    producer.close()
                    #
                    time.sleep(0.1)

    def read_topics(self, source_str_topic_dict_dict, source_or_sink_str):
        source_str_input_record_any_list_dict = defaultdict(list)
        for source_str, topic_dict in source_str_topic_dict_dict.items():
            storage = topic_dict["storage"]
            topic_str = topic_dict["topic"]
            kwargs = topic_dict.get("kwargs", {})
            #
            print(f"Reading {source_or_sink_str} topic {topic_str}...")
            kwargs["group"] = f"test_{get_millis()}"
            message_dict_list = storage.cat(topic_str, **kwargs)
            source_str_input_record_any_list_dict[source_str] = message_dict_list
            print("...done.")
        #
        return source_str_input_record_any_list_dict

    def read_source_topics(self, source_str_topic_dict_dict):
         return self.read_topics(source_str_topic_dict_dict, "source")
    
    def read_sink_topics(self, source_str_topic_dict_dict):
         return self.read_topics(source_str_topic_dict_dict, "sink")
