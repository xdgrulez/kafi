from kafi.storage_consumer import StorageConsumer

# Constants

ALL_MESSAGES = -1

#

class KafkaConsumer(StorageConsumer):
    def __init__(self, kafka_obj, *topics, **kwargs):
        super().__init__(kafka_obj, *topics, **kwargs)

    #

    def foldl(self, foldl_function, initial_acc, n=ALL_MESSAGES, commit_after_processing=None, **kwargs):
        n_int = n
        #
        if n_int == 0:
            return initial_acc
        #
        commit_after_processing_bool = self.storage_obj.commit_after_processing() if commit_after_processing is None else commit_after_processing
        #
        consume_batch_size_int = kwargs["consume_batch_size"] if "consume_batch_size" in kwargs else self.storage_obj.consume_batch_size()
        if n != ALL_MESSAGES and consume_batch_size_int > n_int:
            consume_batch_size_int = n_int
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        topic_str_partitions_int_dict = self.storage_obj.partitions(self.topic_str_list)
        topic_str_offsets_dict_dict = {topic_str: {partition_int: 0 for partition_int in range(partitions_int)} for topic_str, partitions_int in topic_str_partitions_int_dict.items()}
        #
        message_counter_int = 0
        #
        acc = initial_acc
        break_bool = False
        while True:
            message_dict_list = self.consume_impl(n=consume_batch_size_int, **kwargs)
            if not message_dict_list:
                break
            #
            for message_dict in message_dict_list:
                topic_str = message_dict["topic"]
                partition_int = message_dict["partition"]
                offset_int = message_dict["offset"]
                #
                offsets_dict = topic_str_offsets_dict_dict[topic_str]
                #
                offsets_dict[partition_int] = offset_int + 1
                #
                if break_function(acc, message_dict):
                    break_bool = True
                    break
                #
                acc = foldl_function(acc, message_dict)
                message_counter_int += 1
                #
                if self.topic_str_end_offsets_dict_dict is not None and topic_str in self.topic_str_end_offsets_dict_dict:
                    end_offsets_dict = self.topic_str_end_offsets_dict_dict[topic_str]
                    if all(offsets_dict[partition_int] > end_offset_int for partition_int, end_offset_int in end_offsets_dict.items()):
                        break_bool = True
                        break
                #
                if n_int != ALL_MESSAGES and message_counter_int >= n_int:
                    break_bool = True
                    break
            #
            if not self.enable_auto_commit_bool and commit_after_processing_bool:
                self.commit(topic_str_offsets_dict_dict)
            #
            if break_bool:
                break
        #
        return acc

    #

    def consume(self, n=ALL_MESSAGES, **kwargs):
        def foldl_function(message_dict_list, message_dict):
            message_dict_list.append(message_dict)
            #
            return message_dict_list
        #
        return self.foldl(foldl_function, [], n, commit_after_processing=False, **kwargs)
