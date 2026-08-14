from tqdm.auto import tqdm

from kafi.helpers import copy_kwargs

# Constants

ALL_MESSAGES = -1

#

class Functional:
    def foldl(self, topic, foldl_fun, initial_acc, n=ALL_MESSAGES, **kwargs):
        verbose_int = self.verbose()
        #
        consumer = self.consumer(topic, **kwargs)
        #
        progress_num_messages_int = self.progress_num_messages()
        consume_tqdm = tqdm(disable=verbose_int <= 0, unit=" msg", desc="Consuming", miniters=progress_num_messages_int, mininterval=0)
        #
        def foldl_fun1(acc_consume_message_counter_int_tuple, m):
            (acc, consume_message_counter_int) = acc_consume_message_counter_int_tuple
            #
            acc = foldl_fun(acc, m)
            #
            consume_message_counter_int += 1
            #
            consume_tqdm.update(1)
            #
            return (acc, consume_message_counter_int)
        #
        acc_consume_message_counter_int_tuple = consumer.foldl(foldl_fun1, (initial_acc, 0), n, **kwargs)
        #
        consumer.close()
        # Finalize tqdm with a newline.
        if (verbose_int > 0):
            consume_tqdm.close()
            print()
        #
        return acc_consume_message_counter_int_tuple

    #

    def flatmap(self, topic, flatmap_fun, n=ALL_MESSAGES, **kwargs):
        def foldl_fun(list, m):
            list += flatmap_fun(m)
            #
            return list
        #
        return self.foldl(topic, foldl_fun, [], n, **kwargs)

    def map(self, topic, map_fun, n=ALL_MESSAGES, **kwargs):
        def flatmap_fun(m):
            return [map_fun(m)]
        #
        return self.flatmap(topic, flatmap_fun, n, **kwargs)

    def filter(self, topic, filter_fun, n=ALL_MESSAGES, **kwargs):
        def flatmap_fun(m):
            return [m] if filter_fun(m) else []
        #
        return self.flatmap(topic, flatmap_fun, n, **kwargs)

    def foreach(self, topic, foreach_fun, n=ALL_MESSAGES, **kwargs):
        def foldl_fun(_, m):
            foreach_fun(m)
        #
        self.foldl(topic, foldl_fun, None, n, **kwargs)

    #

    def foldl_to(self, topic, target_storage, target_topic, foldl_to_fun, initial_acc, n=ALL_MESSAGES, **kwargs):
        verbose_int = self.verbose()
        #
        progress_num_messages_int = self.progress_num_messages()
        consume_tqdm = tqdm(disable=verbose_int <= 0, unit=" msg", desc="Consuming", miniters=progress_num_messages_int, mininterval=0)
        produce_tqdm = tqdm(disable=verbose_int <= 0, unit=" msg", desc="Producing", miniters=progress_num_messages_int, mininterval=0)
        #
        def foldl_to_fun1(acc_consume_message_counter_int_produce_batch_size_int_produce_batch_m_list_produce_message_counter_int_tuple, m):
            (acc, consume_message_counter_int, produce_batch_size_int, produce_batch_m_list, produce_message_counter_int) = acc_consume_message_counter_int_produce_batch_size_int_produce_batch_m_list_produce_message_counter_int_tuple
            #
            (acc, m_list) = foldl_to_fun(acc, m)
            #
            consume_message_counter_int += 1
            consume_tqdm.update(1)
            #
            produce_batch_m_list += m_list
            #
            if len(produce_batch_m_list) == produce_batch_size_int:
                target_producer.produce_list(produce_batch_m_list, **target_kwargs)
                #
                produced_int = len(produce_batch_m_list)
                produce_message_counter_int += produced_int
                #                
                produce_tqdm.update(produced_int)
                #
                return (acc, consume_message_counter_int, produce_batch_size_int, [], produce_message_counter_int)
            else:
                return (acc, consume_message_counter_int, produce_batch_size_int, produce_batch_m_list, produce_message_counter_int)

        #
        source_kwargs = copy_kwargs("source", **kwargs)
        #
        target_kwargs = copy_kwargs("target", **kwargs)
        #
        produce_batch_size_int = kwargs["produce_batch_size"] if "produce_batch_size" in kwargs else target_storage.produce_batch_size()
        #
        consumer = self.consumer(topic, **source_kwargs)
        #
        target_producer = target_storage.producer(target_topic, **target_kwargs)
        #
        (acc, consume_message_counter_int, _, produce_batch_m_list, produce_message_counter_int) = consumer.foldl(foldl_to_fun1, (initial_acc, 0, produce_batch_size_int, [], 0), n, **kwargs)
        #
        consumer.close()
        #
        if len(produce_batch_m_list) > 0:
            target_producer.produce_list(produce_batch_m_list, **target_kwargs)
            produce_message_counter_int += len(produce_batch_m_list)
        #
        target_producer.close()
        #
        if (verbose_int > 0):
            consume_tqdm.close()
            produce_tqdm.close()
            print()
        #
        return (acc, consume_message_counter_int, produce_message_counter_int)

    def flatmap_to(self, topic, target_storage, target_topic, flatmap_fun, n=ALL_MESSAGES, **kwargs):
        def foldl_to_fun(_, m):
            return (None, flatmap_fun(m))
        #
        (_, consume_message_counter_int, produce_message_counter_int) = self.foldl_to(topic, target_storage, target_topic, foldl_to_fun, None, n, **kwargs)
        return (consume_message_counter_int, produce_message_counter_int)

    def map_to(self, topic, target_storage, target_topic, map_fun, n=ALL_MESSAGES, **kwargs):
        def flatmap_fun(m):
            return [map_fun(m)]
        #
        return self.flatmap_to(topic, target_storage, target_topic, flatmap_fun, n, **kwargs)

    def filter_to(self, topic, target_storage, target_topic, filter_fun, n=ALL_MESSAGES, **kwargs):
        def flatmap_fun(m):
            return [m] if filter_fun(m) else []
        #
        return self.flatmap_to(topic, target_storage, target_topic, flatmap_fun, n, **kwargs)
