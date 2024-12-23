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

    def recreate(self, topic, partitions=None, config={}, **kwargs):
        topic_str = topic
        #
        if self.exists(topic_str):
            if partitions is None:
                partitions_int = self.partitions(topic_str)[topic_str]
            else:
                partitions_int = partitions
            #
            old_config_dict = self.config(topic_str)[topic_str]
            config_dict = {}
            for key_str, value_str in old_config_dict.items():
                if key_str in config:
                    config_dict[key_str] = config[key_str]
                else:
                    config_dict[key_str] = value_str
            #
            self.delete(topic_str)
            #
            self.create(topic_str, partitions=partitions_int, config=config_dict, **kwargs)
        else:
            if partitions is None:
                partitions_int = 1
            else:
                partitions_int = partitions
            #
            self.create(topic_str, partitions=partitions_int, config=config, **kwargs)
        #
        return topic_str

    retouch = recreate

    def join_to(self, source_topic1, source_storage2, source_topic2, target_storage, target_topic, get_key_function1, get_key_function2, projection_function, join="left", n=ALL_MESSAGES, **kwargs):
        join_str = join
        #
        def zip_foldl_to_function(acc, message_dict1, message_dict2):
            print(message_dict1["value"])
            print(message_dict2["value"])
            print("===")
            (index_dict1, index_dict2) = acc
            #
            key1 = get_key_function1(message_dict1)
            key2 = get_key_function2(message_dict2)
            #
            index_dict1[key1] = message_dict1
            index_dict2[key2] = message_dict2
            #
            join_dict = {}
            if join_str == "left":
                for key in index_dict1:
                    if key in index_dict2:
                        join_dict[key1] = projection_function(index_dict1[key1], index_dict2[key1])
                #
                if join_dict == {}:
                    return ((index_dict1, index_dict2), [message_dict1])
                else:
                    return ((index_dict1, index_dict2), [join_dict[key1]])
            #

        return self.zip_foldl_to(source_topic1, source_storage2, source_topic2, target_storage, target_topic, zip_foldl_to_function, ({}, {}), n=n, **kwargs)
