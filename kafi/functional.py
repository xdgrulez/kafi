from tqdm.auto import tqdm

from kafi.helpers import copy_kwargs

# Constants

ALL_MESSAGES = -1

#

class Functional:
    def foldl(self, topic, foldl_fun, initial_acc, n=ALL_MESSAGES, **kwargs):
        """Fold over up to n messages of a topic with a progress bar, then close the consumer.

        Args:
            topic: topic name
            foldl_fun: (acc, m) -> new acc
            initial_acc: starting accumulator value
            n: max messages to consume; ALL_MESSAGES for no limit
            **kwargs: passed to consumer()/consumer.foldl()

        Returns:
            tuple: (final acc, number of messages consumed as int)"""
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
        """Consume a topic and expand each message into zero or more results, collected into a list.

        Args:
            topic: topic name
            flatmap_fun: m -> list of results
            n: max messages to consume; ALL_MESSAGES for no limit
            **kwargs: passed to foldl()

        Returns:
            tuple: (list of results, number of messages consumed as int)"""
        def foldl_fun(list, m):
            list += flatmap_fun(m)
            #
            return list
        #
        return self.foldl(topic, foldl_fun, [], n, **kwargs)

    def map(self, topic, map_fun, n=ALL_MESSAGES, **kwargs):
        """Consume a topic and transform each message into one result, collected into a list.

        Args:
            topic: topic name
            map_fun: m -> result
            n: max messages to consume; ALL_MESSAGES for no limit
            **kwargs: passed to flatmap()

        Returns:
            tuple: (list of results, number of messages consumed as int)"""
        def flatmap_fun(m):
            return [map_fun(m)]
        #
        return self.flatmap(topic, flatmap_fun, n, **kwargs)

    def filter(self, topic, filter_fun, n=ALL_MESSAGES, **kwargs):
        """Consume a topic and keep only messages matching a predicate, collected into a list.

        Args:
            topic: topic name
            filter_fun: m -> bool
            n: max messages to consume; ALL_MESSAGES for no limit
            **kwargs: passed to flatmap()

        Returns:
            tuple: (list of matching m, number of messages consumed as int)"""
        def flatmap_fun(m):
            return [m] if filter_fun(m) else []
        #
        return self.flatmap(topic, flatmap_fun, n, **kwargs)

    def foreach(self, topic, foreach_fun, n=ALL_MESSAGES, **kwargs):
        """Consume a topic, calling a side-effect function per message.

        Args:
            topic: topic name
            foreach_fun: m -> None
            n: max messages to consume; ALL_MESSAGES for no limit
            **kwargs: passed to foldl()"""
        def foldl_fun(_, m):
            foreach_fun(m)
        #
        self.foldl(topic, foldl_fun, None, n, **kwargs)

    #

    def foldl_to(self, topic, target_storage, target_topic, foldl_to_fun, initial_acc, n=ALL_MESSAGES, **kwargs):
        """Fold over a source topic while producing derived messages to a target topic, batched.

        Args:
            topic: source topic name
            target_storage: storage to produce results to
            target_topic: target topic name
            foldl_to_fun: (acc, m) -> (new acc, list of messages to produce)
            initial_acc: starting accumulator value
            n: max messages to consume; ALL_MESSAGES for no limit
            **kwargs: source_*/target_*-prefixed kwargs, split via copy_kwargs()

        Returns:
            tuple: (final acc, number of messages consumed as int, number of messages produced as int)"""
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
        """Consume a source topic, expanding each message into results produced to a target topic.

        Args:
            topic: source topic name
            target_storage: storage to produce results to
            target_topic: target topic name
            flatmap_fun: m -> list of results
            n: max messages to consume; ALL_MESSAGES for no limit
            **kwargs: passed to foldl_to()

        Returns:
            tuple: (number of messages consumed as int, number of messages produced as int)"""
        def foldl_to_fun(_, m):
            return (None, flatmap_fun(m))
        #
        (_, consume_message_counter_int, produce_message_counter_int) = self.foldl_to(topic, target_storage, target_topic, foldl_to_fun, None, n, **kwargs)
        return (consume_message_counter_int, produce_message_counter_int)

    def map_to(self, topic, target_storage, target_topic, map_fun, n=ALL_MESSAGES, **kwargs):
        """Consume a source topic, transforming each message into one result produced to a target topic.

        Args:
            topic: source topic name
            target_storage: storage to produce results to
            target_topic: target topic name
            map_fun: m -> result
            n: max messages to consume; ALL_MESSAGES for no limit
            **kwargs: passed to flatmap_to()

        Returns:
            tuple: (number of messages consumed as int, number of messages produced as int)"""
        def flatmap_fun(m):
            return [map_fun(m)]
        #
        return self.flatmap_to(topic, target_storage, target_topic, flatmap_fun, n, **kwargs)

    def filter_to(self, topic, target_storage, target_topic, filter_fun, n=ALL_MESSAGES, **kwargs):
        """Consume a source topic, producing only messages matching a predicate to a target topic.

        Args:
            topic: source topic name
            target_storage: storage to produce results to
            target_topic: target topic name
            filter_fun: m -> bool
            n: max messages to consume; ALL_MESSAGES for no limit
            **kwargs: passed to flatmap_to()

        Returns:
            tuple: (number of messages consumed as int, number of messages produced as int)"""
        def flatmap_fun(m):
            return [m] if filter_fun(m) else []
        #
        return self.flatmap_to(topic, target_storage, target_topic, flatmap_fun, n, **kwargs)
