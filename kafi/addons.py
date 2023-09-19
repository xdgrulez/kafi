from kafi.functional import Functional

# Constants

ALL_MESSAGES = -1

#

class AddOns(Functional):
    def compact(self, topic, n=ALL_MESSAGES, **kwargs):
        def foldl_function(acc, message_dict):
            key_hash_int_message_dict_dict = acc
            #
            key = message_dict["key"]
            value = message_dict["value"]
            #
            if key is not None:
                key_hash_int = hash(str(key))
                if value is None:
                    if key_hash_int in key_hash_int_message_dict_dict:
                        del key_hash_int_message_dict_dict[key_hash_int]
                else:
                    key_hash_int_message_dict_dict[key_hash_int] = message_dict
            #
            return key_hash_int_message_dict_dict
        #

        (key_hash_int_message_dict_dict, _) = self.foldl(topic, foldl_function, {}, n, **kwargs)
        #
        message_dict_list = list(key_hash_int_message_dict_dict.values())
        #
        return message_dict_list

    def compact_to(self, topic, target_storage, target_topic, n=ALL_MESSAGES, **kwargs):
        source_kwargs = self.get_source_kwargs(**kwargs)
        target_kwargs = self.get_target_kwargs(**kwargs)
        #
        message_dict_list = self.compact(topic, n, **source_kwargs)
        #
        target_producer = target_storage.producer(target_topic, **target_kwargs)
        key_bytes_list_value_bytes_list_tuple = target_producer.produce_list(message_dict_list, **target_kwargs)
        target_producer.close()
        #
        return key_bytes_list_value_bytes_list_tuple

    #

    def recreate(self, topic, partitions=None, **kwargs):
        topic_str = topic
        #
        if partitions is None:
            partitions_int = self.partitions(topic_str)[topic_str]
        else:
            partitions_int = partitions
        #
        old_config_dict = self.config(topic_str)[topic_str]
        #
        self.delete(topic_str)
        #
        return self.create(topic_str, partitions=partitions_int, config=old_config_dict, **kwargs)
