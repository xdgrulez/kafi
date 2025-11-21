from kafi.serializer import Serializer
from kafi.helpers import split_bytes
from kafi.partitioners import default_partitioner, key_to_chunk_key, chunk_key_to_key

# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1

class StorageProducer(Serializer):
    def __init__(self, storage_obj, topic, **kwargs):
        self.storage_obj = storage_obj
        #
        super().__init__(storage_obj.schema_registry_config_dict, **kwargs)
        #
        self.topic_str = topic
        #
        (self.key_type_str, self.value_type_str) = storage_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.key_schema_str_or_dict, self.value_schema_str_or_dict, self.key_schema_id_int, self.value_schema_id_int) = self.get_key_value_schema_tuple(**kwargs)
        #
        self.keep_partitions_bool = kwargs["keep_partitions"] if "keep_partitions" in kwargs else False
        #
        self.keep_timestamps_bool = kwargs["keep_timestamps"] if "keep_timestamps" in kwargs else False
        #
        self.keep_headers_bool = kwargs["keep_headers"] if "keep_headers" in kwargs else True
        #
        self.written_counter_int = 0
        #
        self.schema_hash_int_generalizedProtocolMessageType_dict = {}
        #
        # Cache the number of partitions of the topic (e.g. for custom partitioner functions).
        self.partitions_int = self.storage_obj.partitions(self.topic_str)[self.topic_str]
        # If a custom partitioner function is used, the default projection function just considers the key.
        self.projection_function = kwargs["projection_function"] if "projection_function" in kwargs else lambda x: x["key"]
        #
        self.chunk_size_bytes_int = kwargs["chunk_size_bytes"] if "chunk_size_bytes" in kwargs else -1
        if self.chunk_size_bytes_int == 0:
            raise Exception("Chunk size is zero.")
        if self.chunk_size_bytes_int > 0 and self.__class__.__name__ == "RestProxyProducer":
            raise Exception("Chunking not supported for RestProxy storage.")
        #
        if self.chunk_size_bytes_int > 0:
            self.partitioner_function = default_partitioner
            self.projection_function = chunk_key_to_key

    #

    # Produce a list of messages plus:
    #   * support for self.keep_partitions_bool, self.keep_timestamps_bool and self.keep_headers_bool
    #.  * serialization (except for kafka/RestProxy)
    #   * extensions (e.g. chunking, encryption)
    def produce_list(self, message_dict_list, **kwargs):
        #
        def serialize(payload, key):
            # Do not serialize if this is a RestProxyProducer object (serialization takes place later on the REST Proxy). 
            if self.__class__.__name__ == "RestProxyProducer":
                return payload
            else:
                return self.serialize(payload, key)
        #
        message_dict_list1 = [{"value": serialize(message_dict["value"], False),
                               "key": serialize(message_dict["key"], True),
                               "partition": message_dict["partition"] if self.keep_partitions_bool else RD_KAFKA_PARTITION_UA,
                               "timestamp": message_dict["timestamp"] if self.keep_timestamps_bool else CURRENT_TIME,
                               "headers": message_dict["headers"] if self.keep_headers_bool else None} for message_dict in message_dict_list]
        # (Optional) chunking.
        if self.chunk_size_bytes_int > 0:
            message_dict_list2 = []
            #
            for message_dict1 in message_dict_list1:
                value_bytes = message_dict1["value"]
                #
                if len(value_bytes) > self.chunk_size_bytes_int:
                    chunk_value_bytes_list = split_bytes(value_bytes, self.chunk_size_bytes_int)
                    #
                    for chunk_int, value_bytes in zip(range(len(chunk_value_bytes_list)), chunk_value_bytes_list):
                        key_bytes = key_to_chunk_key(message_dict1["key"], chunk_int)
                        message_dict2 = {"value": value_bytes,
                                         "key": key_bytes,
                                         "partition": message_dict1["partition"],
                                         "timestamp": message_dict1["timestamp"],
                                         "headers": message_dict1["headers"]}
                        message_dict_list2.append(message_dict2)
        else:
            message_dict_list2 = message_dict_list1
        #
        return self.produce_impl(message_dict_list2, **kwargs)

    # Syntactic sugar for produce_list() (including headers).
    def produce(self, value, **kwargs):
        key = kwargs["key"] if "key" in kwargs else None
        partition = kwargs["partition"] if "partition" in kwargs else RD_KAFKA_PARTITION_UA
        timestamp = kwargs["timestamp"] if "timestamp" in kwargs else CURRENT_TIME
        headers = kwargs["headers"] if "headers" in kwargs else None
        #
        value_list = value if isinstance(value, list) else [value]
        #
        key_list = key if isinstance(key, list) else [key for _ in value_list]
        #
        partition_int_list = partition if isinstance(partition, list) else [partition for _ in value_list]
        #
        timestamp_list = timestamp if isinstance(timestamp, list) else [timestamp for _ in value_list]
        #
        headers_list = headers if isinstance(headers, list) and all(self.storage_obj.is_headers(headers1) for headers1 in headers) and len(headers) == len(value_list) else [headers for _ in value_list]
        headers_str_bytes_tuple_list_list = [self.storage_obj.headers_to_headers_str_bytes_tuple_list(headers) for headers in headers_list]
        #
        message_dict_list = [{"value": value,
                              "key": key,
                              "partition": partition_int,
                              "timestamp": timestamp,
                              "headers": headers_str_bytes_tuple_list}
                              for value, key, partition_int, timestamp, headers_str_bytes_tuple_list in zip(value_list, key_list, partition_int_list, timestamp_list, headers_str_bytes_tuple_list_list)]
        #
        self.keep_partitions_bool = True
        self.keep_timestamps_bool = True
        self.keep_headers_bool = True
        #
        return self.produce_list(message_dict_list, **kwargs)

    # Helpers

    def get_key_value_schema_tuple(self, **kwargs):
        key_schema_str_or_dict = kwargs["key_schema"] if "key_schema" in kwargs else None
        value_schema_str_or_dict = kwargs["value_schema"] if "value_schema" in kwargs else None
        #
        key_schema_id_int = kwargs["key_schema_id"] if "key_schema_id" in kwargs else None
        value_schema_id_int = kwargs["value_schema_id"] if "value_schema_id" in kwargs else None
        #
        return (key_schema_str_or_dict, value_schema_str_or_dict, key_schema_id_int, value_schema_id_int)
