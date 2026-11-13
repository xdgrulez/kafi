import threading, time

from streams.test_kafka_base import TestKafkaBase

#

from kafi.helpers import get_millis
from kafi.streams.streams import Streams

CHECKPOINT_ITERATIONS=3

#

class TestStreamsBase(TestKafkaBase):
    def stop(self, source_storage, source_topic_str, batch_size_int, steps_int, group_str, iteration_int):
        group_str_topic_str_offsets_dict_dict_dict = source_storage.group_offsets(group_str)
        if group_str not in group_str_topic_str_offsets_dict_dict_dict:
            return False
        topic_str_offsets_dict_dict = group_str_topic_str_offsets_dict_dict_dict[group_str]
        if source_topic_str not in topic_str_offsets_dict_dict:
            return False
        #
        offsets_dict = topic_str_offsets_dict_dict[source_topic_str]
        offset_int = offsets_dict[0]
        return offset_int >= steps_int * (batch_size_int) * (iteration_int + 1)

    #
    
    def go(self, built_tn, source_str_batch_size_int_dict, steps_int, checkpoint_storage=None, checkpoint_topic_str=None, **kwargs):
        group_str = f"test_{get_millis()}"
        #
        source_str_topic_dict_dict = built_tn.get_source_str_topic_dict_dict()
        sink_str_topic_dict_dict = built_tn.get_sink_str_topic_dict_dict()
        #
        for source_str, topic_dict in source_str_topic_dict_dict.items():
            storage = topic_dict["storage"]
            topic_str = topic_dict["topic"]
            #
            storage.recreate(topic_str)
            #
            for _, topic_dict in sink_str_topic_dict_dict.items():
                storage = topic_dict["storage"]
                topic_str = topic_dict["topic"]
                #                
                storage.recreate(topic_str)
            if checkpoint_storage is not None:
                checkpoint_storage.delete(checkpoint_topic_str)
        #
        for source_str, _ in source_str_topic_dict_dict.items():
            self.init_generate(source_str)
        #
        iterations_int = CHECKPOINT_ITERATIONS if checkpoint_storage is not None else 1
        for iteration_int in range(iterations_int):
            built_tn.reset()
            #
            print(f"Iteration {iteration_int + 1}")
            #
            produce_thread = threading.Thread(target=self.produce, args=(source_str_topic_dict_dict, source_str_batch_size_int_dict, steps_int))
            #
            self.stop_fun = Streams.start_streams(built_tn, checkpoint_storage, checkpoint_topic_str, group=group_str, **kwargs)
            #
            produce_thread.start()
            #
            while True:
                if all(self.stop(topic_dict["storage"], topic_dict["topic"], source_str_batch_size_int_dict[source_str], steps_int, group_str, iteration_int) for source_str, topic_dict in source_str_topic_dict_dict.items()):
                    time.sleep(3)
                    break
                #
                time.sleep(0.1)
            #
            produce_thread.join()
            #
            self.stop_fun()
            #
            if iteration_int < iterations_int - 1:
                time.sleep(3)
        #
        self.source_str_input_r_list_dict = self.read_source_topics(source_str_topic_dict_dict)
        #
        self.sink_str_updated_r_list_dict = self.read_sink_topics(sink_str_topic_dict_dict)
        #
        return group_str
