import re

from kafi.functional import Functional

# Constants

ALL_MESSAGES = -1

#

class Shell(Functional):
    def cat(self, topic, n=ALL_MESSAGES, **kwargs):
        def map_function(message_dict):
            return message_dict
        #
        return self.map(topic, map_function, n, **kwargs)

    def head(self, topic, n=10, **kwargs):
        return self.cat(topic, n, **kwargs)

    def tail(self, topic, n=10, **kwargs):
        topic_str = topic
        n_int = n
        #
        def map_function(message_dict):
            return message_dict
        #
        partitions_int = self.partitions(topic_str)[topic_str]
        offsets_dict = {partition_int: -n_int for partition_int in range(partitions_int)}
        kwargs["offsets"] = offsets_dict
        #
        return self.map(topic, map_function, n, **kwargs)

    #

    def cp(self, source_topic, target_storage, target_topic, map_function=lambda x: x, n=ALL_MESSAGES, **kwargs):
        return self.map_to(source_topic, target_storage, target_topic, map_function, n, **kwargs)

    #

    def wc(self, topic, **kwargs):
        def foldl_function(acc, message_dict):
            if message_dict["key"] is None:
                key_str = ""
            else:
                key_str = str(message_dict["key"])
            num_words_key_int = 0 if key_str == "" else len(key_str.split(" "))
            num_bytes_key_int = len(key_str)
            #
            if message_dict["value"] is None:
                value_str = ""
            else:
                value_str = str(message_dict["value"])
            num_words_value_int = len(value_str.split(" "))
            num_bytes_value_int = len(value_str)
            #
            acc_num_words_int = acc[0] + num_words_key_int + num_words_value_int
            acc_num_bytes_int = acc[1] + num_bytes_key_int + num_bytes_value_int
            return (acc_num_words_int, acc_num_bytes_int)
        #
        ((acc_num_words_int, acc_num_bytes_int), num_messages_int) = self.foldl(topic, foldl_function, (0, 0), **kwargs)
        return (num_messages_int, acc_num_words_int, acc_num_bytes_int)

    #

    def diff_fun(self, topic1, storage2, topic2, diff_function, n=ALL_MESSAGES, **kwargs):
        def zip_foldl_function(acc, message_dict1, message_dict2):
            if diff_function(message_dict1, message_dict2):
                acc += [(message_dict1, message_dict2)]
                #
                if self.verbose() > 0:
                    partition_int1 = message_dict1["partition"]
                    offset_int1 = message_dict1["offset"]
                    partition_int2 = message_dict2["partition"]
                    offset_int2 = message_dict2["offset"]
                    print(f"Found differing messages on 1) partition {partition_int1}, offset {offset_int1} and 2) partition {partition_int2}, offset {offset_int2}.")
                #
            return acc
        #
        return self.zip_foldl(topic1, storage2, topic2, zip_foldl_function, [], n=n, **kwargs)
    
    def diff(self, topic1, storage2, topic2, n=ALL_MESSAGES, **kwargs):
        def diff_function(message_dict1, message_dict2):
            return message_dict1["key"] != message_dict2["key"] or message_dict1["value"] != message_dict2["value"]
        return self.diff_fun(topic1, storage2, topic2, diff_function, n=n, **kwargs)

    #

    def grep_fun(self, topic, match_function, n=ALL_MESSAGES, **kwargs):
        def flatmap_function(message_dict):
            if match_function(message_dict):
                if self.verbose() > 0:
                    partition_int = message_dict["partition"]
                    offset_int = message_dict["offset"]
                    print(f"Found matching message on partition {partition_int}, offset {offset_int}.")
                return [message_dict]
            else:
                return []
        #
        (matching_message_dict_list, message_counter_int) = self.flatmap(topic, flatmap_function, n=n, **kwargs)
        #
        return matching_message_dict_list, len(matching_message_dict_list), message_counter_int

    def grep(self, topic, re_pattern_str, n=ALL_MESSAGES, **kwargs):
        def match_function(message_dict):
            pattern = re.compile(re_pattern_str)
            key_str = str(message_dict["key"])
            value_str = str(message_dict["value"])
            return pattern.match(key_str) is not None or pattern.match(value_str) is not None
        #
        return self.grep_fun(topic, match_function, n=n, **kwargs)

    def stat(self, topic, **kwargs):
        return self.cat(topic, **kwargs)[1]
