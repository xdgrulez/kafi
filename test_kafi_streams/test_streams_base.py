import threading, time

from test_kafi_streams.test_kafka_base import TestKafkaBase

from kafi.helpers import get_millis
from kafi.streams.streams import run_streams

#

class TestStreamsBase(TestKafkaBase):
    def process(self, source_storage_topic_str_tuple_list, sink_storage, sink_topic_str, snapshot_storage, snapshot_topic, root_tn, **kwargs):
        self.stop_function = run_streams(source_storage_topic_str_tuple_list, root_tn, sink_storage, sink_topic_str, snapshot_storage, snapshot_topic, **kwargs)

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
    
    def go(self, root_tn, source_storage_topic_str_batch_size_int_tuple_list, steps_int, sink_storage, sink_topic_str, snapshot_storage=None, snapshot_topic_str=None, recreate_boolean=True, **kwargs):
        group_str = kwargs["group"] if "group" in kwargs else f"test_{get_millis()}"
        kwargs["group"] = group_str
        #
        source_storage_topic_str_tuple_list = [(storage, topic_str) for storage, topic_str, _ in source_storage_topic_str_batch_size_int_tuple_list]
        #
        self.source_str_values_int_dict = {source_str: 0 for _, source_str in source_storage_topic_str_tuple_list}
        #
        group_deleted_boolean = False
        if recreate_boolean:
            for storage, topic_str in source_storage_topic_str_tuple_list:
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
                    storage.delete_groups(group_str)
            #
            sink_storage.recreate(sink_topic_str)
            if snapshot_storage is not None:
                snapshot_storage.recreate(snapshot_topic_str)
        #
        for _, topic_str, _ in source_storage_topic_str_batch_size_int_tuple_list:
            self.init_generate(topic_str)
        #
        thread1 = threading.Thread(target=self.produce, args=(source_storage_topic_str_batch_size_int_tuple_list, steps_int), kwargs=kwargs)
        #
        thread2 = threading.Thread(target=self.process, args=(source_storage_topic_str_tuple_list, sink_storage, sink_topic_str, snapshot_storage, snapshot_topic_str, root_tn), kwargs=kwargs)
        #
        thread1.start()
        thread2.start()
        #
        while True:
            if all(self.stop(storage, topic_str, batch_size_int, steps_int, group_str) for storage, topic_str, batch_size_int in source_storage_topic_str_batch_size_int_tuple_list):
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
        self.read_source_topics(source_storage_topic_str_tuple_list, **kwargs)
        #
        self.read_sink_topic(sink_storage, sink_topic_str, **kwargs)
