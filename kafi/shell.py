import re

from kafi.functional import Functional

# Constants

ALL_MESSAGES = -1

#

class Shell(Functional):
    def cat(self, topic, n=ALL_MESSAGES, map_function=lambda x: x, **kwargs):
        (message_dict_list, _) = self.map(topic, map_function, n, **kwargs)
        return message_dict_list

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
        # watermarks_dict = self.watermarks(topic_str)[topic_str]
        offsets_dict = {partition_int: -n_int for partition_int in range(partitions_int)}
        kwargs["offsets"] = offsets_dict
        #
        (message_dict_list, _) = self.map(topic, map_function, n, **kwargs)
        return message_dict_list

    #

    def cp(self, source_topic, target_storage, target_topic, map_function=lambda x: x, n=ALL_MESSAGES, flatmap_function=None, **kwargs):
        if flatmap_function is not None:
            return self.flatmap_to(source_topic, target_storage, target_topic, flatmap_function, n, **kwargs)
        else:
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

    def grep_function(self, topic, match_function, n=ALL_MESSAGES, matches=ALL_MESSAGES, **kwargs):
        def foldl_function(acc, message_dict):
            (matching_message_dict_acc_list, matches_acc_int) = acc
            if match_function(message_dict):
                if self.verbose() > 0:
                    partition_int = message_dict["partition"]
                    offset_int = message_dict["offset"]
                    print(f"Found matching message on partition {partition_int}, offset {offset_int}.")
                #
                matching_message_dict_acc_list += [message_dict]
                matches_acc_int += 1
                if matches_int != -1 and matches_acc_int >= matches_int:
                    raise Exception(f"Stopped after {matches_int} matches.")
                return (matching_message_dict_acc_list, matches_acc_int)
            else:
                return (matching_message_dict_acc_list, matches_acc_int)
        #
        matches_int = matches
        #
        ((matching_message_dict_list, _), message_counter_int) = self.foldl(topic, foldl_function, ([], 0), n=n, **kwargs)
        #
        return matching_message_dict_list, len(matching_message_dict_list), message_counter_int

    def grep(self, topic, re_pattern_str, n=ALL_MESSAGES, results=ALL_MESSAGES, **kwargs):
        def match_function(message_dict):
            pattern = re.compile(re_pattern_str)
            key_str = str(message_dict["key"])
            value_str = str(message_dict["value"])
            return pattern.match(key_str) is not None or pattern.match(value_str) is not None
        #
        return self.grep_function(topic, match_function, n=n, results=results, **kwargs)
