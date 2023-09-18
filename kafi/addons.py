# Constants

ALL_MESSAGES = -1

#

class AddOns:
    def uncompact_to(self, topic, target_storage, target_topic, n=ALL_MESSAGES, **kwargs):
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

        source_kwargs = self.get_source_kwargs(**kwargs)
        #
        target_kwargs = self.get_target_kwargs(**kwargs)
        #
        (key_hash_int_message_dict_dict, _) = self.foldl(topic, foldl_function, {}, n, **source_kwargs)
        #
        target_producer = target_storage.producer(target_topic, **target_kwargs)
        message_dict_list = list(key_hash_int_message_dict_dict.values())
        key_bytes_list_value_bytes_list_tuple = target_producer.produce_list(message_dict_list, **target_kwargs)
        target_producer.close()
        #
        return key_bytes_list_value_bytes_list_tuple
