import time
from kafi.helpers import zip2

# Constants

ALL_MESSAGES = -1

#

class Functional:
    def foldl(self, topic, foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        verbose_int = self.verbose()
        #
        progress_num_messages_int = self.progress_num_messages()
        #
        consumer = self.consumer(topic, **kwargs)
        #
        def foldl_function1(acc_consume_message_counter_int_tuple, message_dict):
            (acc, consume_message_counter_int) = acc_consume_message_counter_int_tuple
            #
            acc = foldl_function(acc, message_dict)
            #
            consume_message_counter_int += 1
            if verbose_int > 0 and consume_message_counter_int % progress_num_messages_int == 0:
                print(f"Read: {consume_message_counter_int}")
            #
            return (acc, consume_message_counter_int)
        #
        acc_consume_message_counter_int_tuple = consumer.foldl(foldl_function1, (initial_acc, 0), n, **kwargs)
        #
        consumer.close()
        #
        return acc_consume_message_counter_int_tuple

    #

    def flatmap(self, topic, flatmap_function, n=ALL_MESSAGES, **kwargs):
        def foldl_function(list, message_dict):
            list += flatmap_function(message_dict)
            #
            return list
        #
        return self.foldl(topic, foldl_function, [], n, **kwargs)

    def map(self, topic, map_function, n=ALL_MESSAGES, **kwargs):
        def flatmap_function(message_dict):
            return [map_function(message_dict)]
        #
        return self.flatmap(topic, flatmap_function, n, **kwargs)

    def filter(self, topic, filter_function, n=ALL_MESSAGES, **kwargs):
        def flatmap_function(message_dict):
            return [message_dict] if filter_function(message_dict) else []
        #
        return self.flatmap(topic, flatmap_function, n, **kwargs)

    def foreach(self, topic, foreach_function, n=ALL_MESSAGES, **kwargs):
        def foldl_function(_, message_dict):
            foreach_function(message_dict)
        #
        self.foldl(topic, foldl_function, None, n, **kwargs)

    #

    def foldl_to(self, topic, target_storage, target_topic, foldl_to_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        verbose_int = self.verbose()
        #
        progress_num_messages_int = self.progress_num_messages()
        #

        def foldl_to_function1(acc_consume_message_counter_int_produce_batch_size_int_produce_batch_message_dict_list_produce_message_counter_int_tuple, message_dict):
            (acc, consume_message_counter_int, produce_batch_size_int, produce_batch_message_dict_list, produce_message_counter_int) = acc_consume_message_counter_int_produce_batch_size_int_produce_batch_message_dict_list_produce_message_counter_int_tuple
            #
            (acc, message_dict_list) = foldl_to_function(acc, message_dict)
            #
            consume_message_counter_int += 1
            if verbose_int > 0 and consume_message_counter_int % progress_num_messages_int == 0:
                print(f"Read: {consume_message_counter_int}")
            #
            produce_batch_message_dict_list += message_dict_list
            #
            if len(produce_batch_message_dict_list) == produce_batch_size_int:
                target_producer.produce_to(produce_batch_message_dict_list, **target_kwargs)
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
        (acc, consume_message_counter_int, _, produce_batch_message_dict_list, produce_message_counter_int) = consumer.foldl(foldl_to_function1, (initial_acc, 0, produce_batch_size_int, [], 0), n, **kwargs)
        #
        consumer.close()
        #
        if len(produce_batch_message_dict_list) > 0:
            target_producer.produce_to(produce_batch_message_dict_list, **target_kwargs)
            produce_message_counter_int += len(produce_batch_message_dict_list)
        #
        target_producer.close()
        #
        return (acc, consume_message_counter_int, produce_message_counter_int)

    def flatmap_to(self, topic, target_storage, target_topic, flatmap_function, n=ALL_MESSAGES, **kwargs):
        def foldl_to_function(_, message_dict):
            return (None, flatmap_function(message_dict))
        #
        (_, consume_message_counter_int, produce_message_counter_int) = self.foldl_to(topic, target_storage, target_topic, foldl_to_function, None, n, **kwargs)
        return (consume_message_counter_int, produce_message_counter_int)

    def map_to(self, topic, target_storage, target_topic, map_function, n=ALL_MESSAGES, **kwargs):
        def flatmap_function(message_dict):
            return [map_function(message_dict)]
        #
        return self.flatmap_to(topic, target_storage, target_topic, flatmap_function, n, **kwargs)

    def filter_to(self, topic, target_storage, target_topic, filter_function, n=ALL_MESSAGES, **kwargs):
        def flatmap_function(message_dict):
            return [message_dict] if filter_function(message_dict) else []
        #
        return self.flatmap_to(topic, target_storage, target_topic, flatmap_function, n, **kwargs)

    #

    def zip_foldl(self, source1_topic, source2_storage, source2_topic, zip_foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        verbose_int = self.verbose()
        progress_num_messages_int = self.progress_num_messages()
        #
        consume_batch_size_int = kwargs["consume_batch_size"] if "consume_batch_size" in kwargs else self.consume_batch_size()
        if consume_batch_size_int > n_int:
            consume_batch_size_int = n_int
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        source1_kwargs = self.copy_kwargs("source1", **kwargs)
        source2_kwargs = self.copy_kwargs("source2", **kwargs)
        #
        consumer1 = self.consumer(source1_topic, **source1_kwargs)
        consumer2 = source2_storage.consumer(source2_topic, **source2_kwargs)
        #
        message_counter_int1 = 0
        message_counter_int2 = 0
        acc = initial_acc
        break_bool = False
        while True:
            message_dict_list1 = []
            while True:
                message_dict_list1 += consumer1.consume(n=consume_batch_size_int)
                if not message_dict_list1 or consume_batch_size_int == ALL_MESSAGES or len(message_dict_list1) == consume_batch_size_int:
                    break
            if not message_dict_list1:
                break
            num_messages_int1 = len(message_dict_list1)
            message_counter_int1 += num_messages_int1
            if verbose_int > 0 and message_counter_int1 % progress_num_messages_int == 0:
                print(f"Read (storage 1): {message_counter_int1}")
            #
            consume_batch_size_int2 = num_messages_int1 if num_messages_int1 < consume_batch_size_int else consume_batch_size_int
            message_dict_list2 = []
            while True:
                message_dict_list2 += consumer2.consume(n=consume_batch_size_int2)
                if not message_dict_list2 or consume_batch_size_int2 == ALL_MESSAGES or len(message_dict_list2) == consume_batch_size_int2:
                    break
            if not message_dict_list2:
                break
            num_messages_int2 = len(message_dict_list2)
            message_counter_int2 += num_messages_int2
            if verbose_int > 0 and message_counter_int2 % progress_num_messages_int == 0:
                print(f"Read (storage 2): {message_counter_int2}")
            #
            for message_dict1, message_dict2 in zip2(message_dict_list1, message_dict_list2):
                if break_function(message_dict1, message_dict2):
                    break_bool = True
                    break
                acc = zip_foldl_function(acc, message_dict1, message_dict2)
            #
            if break_bool:
                break
            #
            if n_int != ALL_MESSAGES:
                if message_counter_int1 >= n_int:
                    break
        #
        consumer1.close()
        consumer2.close()
        #
        return acc, message_counter_int1, message_counter_int2

    #

    def zip_foldl_to(self, source1_topic, source2_storage, source2_topic, target_storage, target_topic, zip_foldl_to_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        verbose_int = self.verbose()
        progress_num_messages_int = self.progress_num_messages()
        #
        consume_batch_size_int = kwargs["consume_batch_size"] if "consume_batch_size" in kwargs else self.consume_batch_size()
        if consume_batch_size_int > n_int:
            consume_batch_size_int = n_int
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        source1_kwargs = self.copy_kwargs("source1", **kwargs)
        source2_kwargs = self.copy_kwargs("source2", **kwargs)
        #
        target_kwargs = self.copy_kwargs("target", **kwargs)
        #
        produce_batch_size_int = kwargs["produce_batch_size"] if "produce_batch_size" in kwargs else target_storage.produce_batch_size()
        #
        consumer1 = self.consumer(source1_topic, **source1_kwargs)
        consumer2 = source2_storage.consumer(source2_topic, **source2_kwargs)
        #
        target_producer = target_storage.producer(target_topic, **target_kwargs)
        #
        message_counter_int1 = 0
        message_counter_int2 = 0
        acc = initial_acc
        break_bool = False
        produce_batch_message_dict_list = []
        produce_message_counter_int = 0
        while True:
            message_dict_list1 = []
            while True:
                message_dict_list1 += consumer1.consume(n=consume_batch_size_int)
                if not message_dict_list1 or consume_batch_size_int == ALL_MESSAGES or len(message_dict_list1) == consume_batch_size_int:
                    break
            if not message_dict_list1:
                break
            num_messages_int1 = len(message_dict_list1)
            message_counter_int1 += num_messages_int1
            if verbose_int > 0 and message_counter_int1 % progress_num_messages_int == 0:
                print(f"Read (storage 1): {message_counter_int1}")
            #
            consume_batch_size_int2 = num_messages_int1 if num_messages_int1 < consume_batch_size_int else consume_batch_size_int
            message_dict_list2 = []
            while True:
                message_dict_list2 += consumer2.consume(n=consume_batch_size_int2)
                if not message_dict_list2 or consume_batch_size_int2 == ALL_MESSAGES or len(message_dict_list2) == consume_batch_size_int2:
                    break
            if not message_dict_list2:
                break
            num_messages_int2 = len(message_dict_list2)
            message_counter_int2 += num_messages_int2
            if verbose_int > 0 and message_counter_int2 % progress_num_messages_int == 0:
                print(f"Read (storage 2): {message_counter_int2}")
            #
            collected_message_dict_list = []
            for message_dict1, message_dict2 in zip2(message_dict_list1, message_dict_list2):
                if break_function(message_dict1, message_dict2):
                    break_bool = True
                    break
                (acc, message_dict_list) = zip_foldl_to_function(acc, message_dict1, message_dict2)
                collected_message_dict_list += message_dict_list
            #
            if break_bool:
                break
            #
            produce_batch_message_dict_list += collected_message_dict_list
            #
            if len(produce_batch_message_dict_list) == produce_batch_size_int:
                target_producer.produce_to(produce_batch_message_dict_list, **target_kwargs)
                #
                produce_message_counter_int += len(produce_batch_message_dict_list)
                if verbose_int > 0 and produce_message_counter_int % progress_num_messages_int == 0:
                    print(f"Written: {produce_message_counter_int}")
                #
                produce_batch_message_dict_list = []
            #
            if n_int != ALL_MESSAGES:
                if message_counter_int1 >= n_int:
                    break
        #
        consumer1.close()
        consumer2.close()
        #
        if len(produce_batch_message_dict_list) > 0:
            target_producer.produce_to(produce_batch_message_dict_list, **target_kwargs)
            #
            produce_message_counter_int += len(produce_batch_message_dict_list)
        #
        target_producer.close()
        #
        return acc, message_counter_int1, message_counter_int2, produce_message_counter_int

    #

    def zip_foldl(self, source1_topic, source2_storage, source2_topic, zip_foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        verbose_int = self.verbose()
        progress_num_messages_int = self.progress_num_messages()
        #
        consume_batch_size_int = kwargs["consume_batch_size"] if "consume_batch_size" in kwargs else self.consume_batch_size()
        if consume_batch_size_int > n_int:
            consume_batch_size_int = n_int
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        source1_kwargs = self.copy_kwargs("source1", **kwargs)
        source2_kwargs = self.copy_kwargs("source2", **kwargs)
        #
        consumer1 = self.consumer(source1_topic, **source1_kwargs)
        consumer2 = source2_storage.consumer(source2_topic, **source2_kwargs)
        #
        message_counter_int1 = 0
        message_counter_int2 = 0
        acc = initial_acc
        break_bool = False
        while True:
            message_dict_list1 = []
            while True:
                message_dict_list1 += consumer1.consume(n=consume_batch_size_int)
                if not message_dict_list1 or consume_batch_size_int == ALL_MESSAGES or len(message_dict_list1) == consume_batch_size_int:
                    break
            if not message_dict_list1:
                break
            num_messages_int1 = len(message_dict_list1)
            message_counter_int1 += num_messages_int1
            if verbose_int > 0 and message_counter_int1 % progress_num_messages_int == 0:
                print(f"Read (storage 1): {message_counter_int1}")
            #
            consume_batch_size_int2 = num_messages_int1 if num_messages_int1 < consume_batch_size_int else consume_batch_size_int
            message_dict_list2 = []
            while True:
                message_dict_list2 += consumer2.consume(n=consume_batch_size_int2)
                if not message_dict_list2 or consume_batch_size_int2 == ALL_MESSAGES or len(message_dict_list2) == consume_batch_size_int2:
                    break
            if not message_dict_list2:
                break
            num_messages_int2 = len(message_dict_list2)
            message_counter_int2 += num_messages_int2
            if verbose_int > 0 and message_counter_int2 % progress_num_messages_int == 0:
                print(f"Read (storage 2): {message_counter_int2}")
            #
            for message_dict1, message_dict2 in zip2(message_dict_list1, message_dict_list2):
                if break_function(message_dict1, message_dict2):
                    break_bool = True
                    break
                acc = zip_foldl_function(acc, message_dict1, message_dict2)
            #
            if break_bool:
                break
            #
            if n_int != ALL_MESSAGES:
                if message_counter_int1 >= n_int:
                    break
        #
        consumer1.close()
        consumer2.close()
        #
        return acc, message_counter_int1, message_counter_int2

    #

    def zip_foldl_to(self, source1_topic, source2_storage, source2_topic, target_storage, target_topic, zip_foldl_to_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        verbose_int = self.verbose()
        progress_num_messages_int = self.progress_num_messages()
        #
        consume_batch_size_int = kwargs["consume_batch_size"] if "consume_batch_size" in kwargs else self.consume_batch_size()
        if consume_batch_size_int > n_int:
            consume_batch_size_int = n_int
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        source1_kwargs = self.copy_kwargs("source1", **kwargs)
        source2_kwargs = self.copy_kwargs("source2", **kwargs)
        #
        target_kwargs = self.copy_kwargs("target", **kwargs)
        #
        produce_batch_size_int = kwargs["produce_batch_size"] if "produce_batch_size" in kwargs else target_storage.produce_batch_size()
        #
        consumer1 = self.consumer(source1_topic, **source1_kwargs)
        consumer2 = source2_storage.consumer(source2_topic, **source2_kwargs)
        #
        target_producer = target_storage.producer(target_topic, **target_kwargs)
        #
        message_counter_int1 = 0
        message_counter_int2 = 0
        acc = initial_acc
        break_bool = False
        produce_batch_message_dict_list = []
        produce_message_counter_int = 0
        while True:
            message_dict_list1 = []
            while True:
                message_dict_list1 += consumer1.consume(n=consume_batch_size_int)
                if not message_dict_list1 or consume_batch_size_int == ALL_MESSAGES or len(message_dict_list1) == consume_batch_size_int:
                    break
            if not message_dict_list1:
                break
            num_messages_int1 = len(message_dict_list1)
            message_counter_int1 += num_messages_int1
            if verbose_int > 0 and message_counter_int1 % progress_num_messages_int == 0:
                print(f"Read (storage 1): {message_counter_int1}")
            #
            consume_batch_size_int2 = num_messages_int1 if num_messages_int1 < consume_batch_size_int else consume_batch_size_int
            message_dict_list2 = []
            while True:
                message_dict_list2 += consumer2.consume(n=consume_batch_size_int2)
                if not message_dict_list2 or consume_batch_size_int2 == ALL_MESSAGES or len(message_dict_list2) == consume_batch_size_int2:
                    break
            if not message_dict_list2:
                break
            num_messages_int2 = len(message_dict_list2)
            message_counter_int2 += num_messages_int2
            if verbose_int > 0 and message_counter_int2 % progress_num_messages_int == 0:
                print(f"Read (storage 2): {message_counter_int2}")
            #
            collected_message_dict_list = []
            for message_dict1, message_dict2 in zip2(message_dict_list1, message_dict_list2):
                if break_function(message_dict1, message_dict2):
                    break_bool = True
                    break
                (acc, message_dict_list) = zip_foldl_to_function(acc, message_dict1, message_dict2)
                collected_message_dict_list += message_dict_list
            #
            if break_bool:
                break
            #
            produce_batch_message_dict_list += collected_message_dict_list
            #
            if len(produce_batch_message_dict_list) == produce_batch_size_int:
                target_producer.produce_to(produce_batch_message_dict_list, **target_kwargs)
                #
                produce_message_counter_int += len(produce_batch_message_dict_list)
                if verbose_int > 0 and produce_message_counter_int % progress_num_messages_int == 0:
                    print(f"Written: {produce_message_counter_int}")
                #
                produce_batch_message_dict_list = []
            #
            if n_int != ALL_MESSAGES:
                if message_counter_int1 >= n_int:
                    break
        #
        consumer1.close()
        consumer2.close()
        #
        if len(produce_batch_message_dict_list) > 0:
            target_producer.produce_to(produce_batch_message_dict_list, **target_kwargs)
            #
            produce_message_counter_int += len(produce_batch_message_dict_list)
        #
        target_producer.close()
        #
        return acc, message_counter_int1, message_counter_int2, produce_message_counter_int

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
