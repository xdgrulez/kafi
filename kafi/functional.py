# import time
from kafi.helpers import zip2

# Constants

ALL_MESSAGES = -1

#

class Functional:
    def foldl(self, topic, foldl_fun, initial_acc, n=ALL_MESSAGES, **kwargs):
        verbose_int = self.verbose()
        #
        progress_num_messages_int = self.progress_num_messages()
        #
        consumer = self.consumer(topic, **kwargs)
        #
        def foldl_fun1(acc_consume_message_counter_int_tuple, message_dict):
            (acc, consume_message_counter_int) = acc_consume_message_counter_int_tuple
            #
            acc = foldl_fun(acc, message_dict)
            #
            consume_message_counter_int += 1
            if verbose_int > 0 and consume_message_counter_int % progress_num_messages_int == 0:
                print(f"Read: {consume_message_counter_int}")
            #
            return (acc, consume_message_counter_int)
        #
        acc_consume_message_counter_int_tuple = consumer.foldl(foldl_fun1, (initial_acc, 0), n, **kwargs)
        #
        consumer.close()
        #
        return acc_consume_message_counter_int_tuple

    #

    def flatmap(self, topic, flatmap_fun, n=ALL_MESSAGES, **kwargs):
        def foldl_fun(list, message_dict):
            list += flatmap_fun(message_dict)
            #
            return list
        #
        return self.foldl(topic, foldl_fun, [], n, **kwargs)

    def map(self, topic, map_fun, n=ALL_MESSAGES, **kwargs):
        def flatmap_fun(message_dict):
            return [map_fun(message_dict)]
        #
        return self.flatmap(topic, flatmap_fun, n, **kwargs)

    def filter(self, topic, filter_fun, n=ALL_MESSAGES, **kwargs):
        def flatmap_fun(message_dict):
            return [message_dict] if filter_fun(message_dict) else []
        #
        return self.flatmap(topic, flatmap_fun, n, **kwargs)

    def foreach(self, topic, foreach_fun, n=ALL_MESSAGES, **kwargs):
        def foldl_fun(_, message_dict):
            foreach_fun(message_dict)
        #
        self.foldl(topic, foldl_fun, None, n, **kwargs)

    #

    def foldl_to(self, topic, target_storage, target_topic, foldl_to_fun, initial_acc, n=ALL_MESSAGES, **kwargs):
        verbose_int = self.verbose()
        #
        progress_num_messages_int = self.progress_num_messages()
        #

        def foldl_to_fun1(acc_consume_message_counter_int_produce_batch_size_int_produce_batch_message_dict_list_produce_message_counter_int_tuple, message_dict):
            (acc, consume_message_counter_int, produce_batch_size_int, produce_batch_message_dict_list, produce_message_counter_int) = acc_consume_message_counter_int_produce_batch_size_int_produce_batch_message_dict_list_produce_message_counter_int_tuple
            #
            (acc, message_dict_list) = foldl_to_fun(acc, message_dict)
            #
            consume_message_counter_int += 1
            if verbose_int > 0 and consume_message_counter_int % progress_num_messages_int == 0:
                print(f"Read: {consume_message_counter_int}")
            #
            produce_batch_message_dict_list += message_dict_list
            #
            if len(produce_batch_message_dict_list) == produce_batch_size_int:
                target_producer.produce_list(produce_batch_message_dict_list, **target_kwargs)
                #
                produce_message_counter_int += len(produce_batch_message_dict_list)
                if verbose_int > 0 and produce_message_counter_int % progress_num_messages_int == 0:
                    print(f"Written: {produce_message_counter_int}")
                #
                return (acc, consume_message_counter_int, produce_batch_size_int, [], produce_message_counter_int)
            else:
                return (acc, consume_message_counter_int, produce_batch_size_int, produce_batch_message_dict_list, produce_message_counter_int)

        #
        source_kwargs = self.copy_kwargs("source", **kwargs)
        #
        target_kwargs = self.copy_kwargs("target", **kwargs)
        #
        produce_batch_size_int = kwargs["produce_batch_size"] if "produce_batch_size" in kwargs else target_storage.produce_batch_size()
        #
        consumer = self.consumer(topic, **source_kwargs)
        #
        target_producer = target_storage.producer(target_topic, **target_kwargs)
        #
        (acc, consume_message_counter_int, _, produce_batch_message_dict_list, produce_message_counter_int) = consumer.foldl(foldl_to_fun1, (initial_acc, 0, produce_batch_size_int, [], 0), n, **kwargs)
        #
        consumer.close()
        #
        if len(produce_batch_message_dict_list) > 0:
            target_producer.produce_list(produce_batch_message_dict_list, **target_kwargs)
            produce_message_counter_int += len(produce_batch_message_dict_list)
        #
        target_producer.close()
        #
        return (acc, consume_message_counter_int, produce_message_counter_int)

    def flatmap_to(self, topic, target_storage, target_topic, flatmap_fun, n=ALL_MESSAGES, **kwargs):
        def foldl_to_fun(_, message_dict):
            return (None, flatmap_fun(message_dict))
        #
        (_, consume_message_counter_int, produce_message_counter_int) = self.foldl_to(topic, target_storage, target_topic, foldl_to_fun, None, n, **kwargs)
        return (consume_message_counter_int, produce_message_counter_int)

    def map_to(self, topic, target_storage, target_topic, map_fun, n=ALL_MESSAGES, **kwargs):
        def flatmap_fun(message_dict):
            return [map_fun(message_dict)]
        #
        return self.flatmap_to(topic, target_storage, target_topic, flatmap_fun, n, **kwargs)

    def filter_to(self, topic, target_storage, target_topic, filter_fun, n=ALL_MESSAGES, **kwargs):
        def flatmap_fun(message_dict):
            return [message_dict] if filter_fun(message_dict) else []
        #
        return self.flatmap_to(topic, target_storage, target_topic, flatmap_fun, n, **kwargs)

    #

    def copy_kwargs(self, name_str, **kwargs):
        copied_kwargs = kwargs.copy()
        #
        if f"{name_str}_group" in kwargs:
            copied_kwargs["group"] = kwargs[f"{name_str}_group"]
        if f"{name_str}_offsets" in kwargs:
            copied_kwargs["offsets"] = kwargs[f"{name_str}_offsets"]
        if f"{name_str}_key_type" in kwargs:
            copied_kwargs["key_type"] = kwargs[f"{name_str}_key_type"]
        if f"{name_str}_value_type" in kwargs:
            copied_kwargs["value_type"] = kwargs[f"{name_str}_value_type"]
        if f"{name_str}_type" in kwargs:
            copied_kwargs["type"] = kwargs[f"{name_str}_type"]
        if f"{name_str}_key_schema" in kwargs:
            copied_kwargs["key_schema"] = kwargs[f"{name_str}_key_schema"]
        if f"{name_str}_value_schema" in kwargs:
            copied_kwargs["value_schema"] = kwargs[f"{name_str}_value_schema"]
        if f"{name_str}_key_schema_id" in kwargs:
            copied_kwargs["key_schema_id"] = kwargs[f"{name_str}_key_schema_id"]
        if f"{name_str}_value_schema_id" in kwargs:
            copied_kwargs["value_schema_id"] = kwargs[f"{name_str}_value_schema_id"]
        #
        return copied_kwargs
