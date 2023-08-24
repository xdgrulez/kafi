from kafi.helpers import get_millis

from kafi.storage_consumer import StorageConsumer

# Constants

ALL_MESSAGES = -1

#

class KafkaConsumer(StorageConsumer):
    def __init__(self, kafka_obj, *topics, **kwargs):
        super().__init__(kafka_obj, *topics, **kwargs)

    #

    def foldl(self, foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        consume_batch_size_int = kwargs["consume_batch_size"] if "consume_batch_size" in kwargs else self.storage_obj.consume_batch_size()
        if n != ALL_MESSAGES and consume_batch_size_int > n_int:
            consume_batch_size_int = n_int
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        message_counter_int = 0
        #
        acc = initial_acc
        break_bool = False
        while True:
            message_dict_list = self.consume(n=consume_batch_size_int, **kwargs)
            if not message_dict_list:
                break
            #
            topic_str_partition_int_offset_int_dict_dict = {}
            for message_dict in message_dict_list:
                if message_dict["topic"] not in topic_str_partition_int_offset_int_dict_dict:
                    topic_str_partition_int_offset_int_dict_dict[message_dict["topic"]] = {}
                #
                partition_int_offset_int_dict = topic_str_partition_int_offset_int_dict_dict[message_dict["topic"]]
                partition_int_offset_int_dict[message_dict["partition"]] = message_dict["offset"] + 1
                #
                if break_function(acc, message_dict):
                    break_bool = True
                    break
                acc = foldl_function(acc, message_dict)
                message_counter_int += 1
            #
            self.commit(topic_str_partition_int_offset_int_dict_dict)
            #
            if break_bool:
                break
            #
            if n_int != ALL_MESSAGES:
                if message_counter_int >= n_int:
                    break
        #
        return acc

    #

    def read(self, n=ALL_MESSAGES):
        def foldl_function(message_dict_list, message_dict):
            message_dict_list.append(message_dict)
            #
            return message_dict_list
        #
        return self.foldl(foldl_function, [], n)
