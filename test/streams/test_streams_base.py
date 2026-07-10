import threading, time

from streams.test_kafka_base import TestKafkaBase

#

from kafi.helpers import get_millis
from kafi.streams.streams import start_streams, get_source_str_topic_dict_dict, get_sink_str_topic_dict_dict

#

class TestStreamsBase(TestKafkaBase):
    def process(self, built_tn, checkpoint_storage, checkpoint_topic, **kwargs):
        self.stop_function = start_streams(built_tn, checkpoint_storage, checkpoint_topic, **kwargs)

    #

    def stop(self, source_storage, source_topic_str, batch_size_int, steps_int, group_str):
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
    
    def go(self, built_tn, source_str_batch_size_int_dict, steps_int, checkpoint_storage=None, checkpoint_topic_str=None, recreate_boolean=True, **kwargs):
        if not "group" in kwargs:
            group_str = f"test_{get_millis()}"
            kwargs["group"] = group_str
        else:
            group_str = kwargs["group"]
        #
        source_str_topic_dict_dict = get_source_str_topic_dict_dict(built_tn)
        sink_str_topic_dict_dict = get_sink_str_topic_dict_dict(built_tn)
        #
        group_deleted_boolean = False
        if recreate_boolean:
            for source_str, topic_dict in source_str_topic_dict_dict.items():
                storage = topic_dict["storage"]
                topic_str = topic_dict["topic"]
                #
                storage.recreate(topic_str)
                #
                if not group_deleted_boolean:
                    while True:
                        group_str_group_description_dict_dict = storage.describe_groups(group_str)
                        if group_str in group_str_group_description_dict_dict:
                            if group_str_group_description_dict_dict[group_str]["state"] == "empty":
                                group_deleted_boolean = True
                                break
                        else:
                            break
                        #
                        time.sleep(0.1)
                    #
                    storage.delete_groups(group_str)
            #
            for _, topic_dict in sink_str_topic_dict_dict.items():
                storage = topic_dict["storage"]
                topic_str = topic_dict["topic"]
                #                
                storage.recreate(topic_str)
            if checkpoint_storage is not None:
                checkpoint_storage.recreate(checkpoint_topic_str)
        #
        for source_str, _ in source_str_topic_dict_dict.items():
            self.init_generate(source_str)
        #
        thread1 = threading.Thread(target=self.produce, args=(source_str_topic_dict_dict, source_str_batch_size_int_dict, steps_int))
        #
        thread2 = threading.Thread(target=self.process, args=(built_tn, checkpoint_storage, checkpoint_topic_str), kwargs=kwargs)
        #
        thread1.start()
        thread2.start()
        #
        while True:
            if all(self.stop(topic_dict["storage"], topic_dict["topic"], source_str_batch_size_int_dict[source_str], steps_int, group_str) for source_str, topic_dict in source_str_topic_dict_dict.items()):
                time.sleep(5)
                break
            #
            time.sleep(0.1)
        #
        self.stop_function()
        #
        thread1.join()
        thread2.join()
        #
        self.source_str_input_record_any_list_dict = self.read_source_topics(source_str_topic_dict_dict)
        #
        self.sink_str_updated_record_any_list_dict = self.read_sink_topics(sink_str_topic_dict_dict)
