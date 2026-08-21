import re

from tqdm.auto import tqdm

from kafi.functional import Functional

# Constants

ALL_MESSAGES = -1

#

class Shell(Functional):
    def cat(self, topic, n=ALL_MESSAGES, map_fun=lambda x: x, **kwargs):
        """Consume up to n messages of a topic, optionally transformed, as a list.

        Args:
            topic: topic name
            n: max messages to consume; ALL_MESSAGES for no limit
            map_fun: m -> result, applied to each message
            **kwargs: passed to map()"""
        (m_list, _) = self.map(topic, map_fun, n, **kwargs)
        return m_list

    def head(self, topic, n=10, **kwargs):
        """First n messages of a topic (from the start, or from configured offsets).

        Args:
            topic: topic name
            n: number of messages
            **kwargs: passed to cat()"""
        return self.cat(topic, n, **kwargs)

    def tail(self, topic, n=10, **kwargs):
        """Last n messages of a topic (per partition, via negative offsets).

        Args:
            topic: topic name
            n: number of trailing messages per partition
            **kwargs: passed to map()"""
        topic_str = topic
        n_int = n
        #
        def map_fun(m):
            return m
        #
        partitions_int = self.partitions(topic_str)[topic_str]
        # watermarks_dict = self.watermarks(topic_str)[topic_str]
        offsets_dict = {partition_int: -n_int for partition_int in range(partitions_int)}
        kwargs["offsets"] = offsets_dict
        #
        (m_list, _) = self.map(topic, map_fun, n, **kwargs)
        return m_list

    #

    def cp(self, source_topic, target_storage, target_topic, map_fun=lambda x: x, n=ALL_MESSAGES, flatmap_fun=None, **kwargs):
        """Copy (optionally transforming/expanding) messages from one topic to another, possibly cross-storage.

        Args:
            source_topic: source topic name
            target_storage: storage to produce results to
            target_topic: target topic name
            map_fun: m -> result, used if flatmap_fun is not given
            n: max messages to consume; ALL_MESSAGES for no limit
            flatmap_fun: m -> list of results; if given, takes precedence over map_fun
            **kwargs: passed to map_to()/flatmap_to()"""
        if flatmap_fun is not None:
            return self.flatmap_to(source_topic, target_storage, target_topic, flatmap_fun, n, **kwargs)
        else:
            return self.map_to(source_topic, target_storage, target_topic, map_fun, n, **kwargs)

    #

    def wc(self, topic, **kwargs):
        """Word/byte counts across a topic's keys and values.

        Args:
            topic: topic name
            **kwargs: passed to foldl()"""
        def foldl_fun(acc, m):
            if m["key"] is None:
                key_str = ""
            else:
                key_str = str(m["key"])
            num_words_key_int = 0 if key_str == "" else len(key_str.split(" "))
            num_bytes_key_int = len(key_str)
            #
            if m["value"] is None:
                value_str = ""
            else:
                value_str = str(m["value"])
            num_words_value_int = len(value_str.split(" "))
            num_bytes_value_int = len(value_str)
            #
            acc_num_words_int = acc[0] + num_words_key_int + num_words_value_int
            acc_num_bytes_int = acc[1] + num_bytes_key_int + num_bytes_value_int
            return (acc_num_words_int, acc_num_bytes_int)
        #
        ((acc_num_words_int, acc_num_bytes_int), num_messages_int) = self.foldl(topic, foldl_fun, (0, 0), **kwargs)
        return (num_messages_int, acc_num_words_int, acc_num_bytes_int)

    #

    def grep_fun(self, topic, match_fun, n=ALL_MESSAGES, matches=ALL_MESSAGES, **kwargs):
        """Collect messages matching an arbitrary predicate, stopping early after enough matches.

        Args:
            topic: topic name
            match_fun: m -> bool
            n: max messages to consume; ALL_MESSAGES for no limit
            matches: stop after this many matches; ALL_MESSAGES for no limit
            **kwargs: passed to foldl()"""
        def foldl_fun(acc, m):
            (matching_m_acc_list, matches_acc_int) = acc
            if match_fun(m):
                if self.verbose() > 0:
                    partition_int = m["partition"]
                    offset_int = m["offset"]
                    tqdm.write(f"Found matching message on partition {partition_int}, offset {offset_int}.")
                #
                matching_m_acc_list += [m]
                matches_acc_int += 1
                if matches_int != -1 and matches_acc_int >= matches_int:
                    raise Exception(f"Stopped after {matches_int} matches.")
                return (matching_m_acc_list, matches_acc_int)
            else:
                return (matching_m_acc_list, matches_acc_int)
        #
        matches_int = matches
        #
        ((matching_m_list, _), message_counter_int) = self.foldl(topic, foldl_fun, ([], 0), n=n, **kwargs)
        #
        return matching_m_list, len(matching_m_list), message_counter_int

    def grep(self, topic, re_pattern_str, n=ALL_MESSAGES, results=ALL_MESSAGES, **kwargs):
        """Collect messages whose key or value matches a regex pattern.

        Args:
            topic: topic name
            re_pattern_str: regular expression to match key/value against
            n: max messages to consume; ALL_MESSAGES for no limit
            results: stop after this many matches; ALL_MESSAGES for no limit
            **kwargs: passed to grep_fun()"""
        def match_fun(m):
            pattern = re.compile(re_pattern_str)
            key_str = str(m["key"])
            value_str = str(m["value"])
            return pattern.match(key_str) is not None or pattern.match(value_str) is not None
        #
        return self.grep_fun(topic, match_fun, n=n, results=results, **kwargs)

    def stat(self, topic, **kwargs):
        """Number of messages returned by cat() for a topic.

        Args:
            topic: topic name
            **kwargs: passed to cat()"""
        return self.cat(topic, **kwargs)[1]
