from kafi.chunker import Chunker

# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1

class StorageProducer(Chunker):
    def __init__(self, storage_obj, topic, **kwargs):
        """Resolve key/value types, schemas, and produce-time flags for a new producer.

        Args:
            storage_obj: owning storage instance
            topic: target topic name
            **kwargs: type/key_type/value_type, key_schema(_id)/value_schema(_id), keep_partitions, keep_timestamps, keep_headers"""
        self.storage_obj = storage_obj
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
        if len(self.storage_obj.admin.list_topics(self.topic_str)) > 0:
            self.partitions_int = self.storage_obj.partitions(self.topic_str)[self.topic_str]
        else:
            self.partitions_int = 1
        #
        super().__init__(storage_obj.schema_registry_config_dict, **kwargs)

    #

    # Produce a list of messages plus:
    #   * support for self.keep_partitions_bool, self.keep_timestamps_bool and self.keep_headers_bool
    #.  * serialization (except for kafka/RestProxy)
    #   * extensions (e.g. chunking, encryption)
    def produce_list(self, m_list, **kwargs):
        """Serialize, chunk, and produce a list of messages.

        Args:
            m_list: messages to produce; each a dict with value and optionally key/partition/timestamp/headers
            **kwargs: passed to produce_impl()

        Returns:
            the result of produce_impl() (implementation-specific: e.g. per-message delivery info)"""
        #
        def serialize(payload, key_bool):
            # Do not serialize if this is a RestProxyProducer object (serialization takes place later on the REST Proxy). 
            if self.__class__.__name__ == "RestProxyProducer":
                return payload
            else:
                return self.serialize(payload, key_bool)
        #
        m_list1 = [{"value": serialize(m["value"], False),
                    "key": serialize(m["key"] if "key" in m else None, True),
                    "partition": m["partition"] if "partition" in m and self.keep_partitions_bool else RD_KAFKA_PARTITION_UA,
                    "timestamp": m["timestamp"] if "timestamp" in m and self.keep_timestamps_bool else CURRENT_TIME,
                    "headers": self.storage_obj.headers_to_headers_str_bytes_tuple_list(m["headers"]) if "headers" in m and self.keep_headers_bool else None} for m in m_list]
        #
        m_list2 = self.chunk(m_list1)
        #
        return self.produce_impl(m_list2, **kwargs)

    # Syntactic sugar for produce_list() (including headers).
    def produce(self, value, **kwargs):
        """Syntactic sugar over produce_list() for one or more values with matching key/partition/timestamp/headers.

        Args:
            value: a single value, or a list of values to produce
            **kwargs: "key", "partition", "timestamp", "headers" (each a single value or a list matching value's length)

        Returns:
            the result of produce_list() (implementation-specific: e.g. per-message delivery info)"""
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
        m_list = [{"value": value,
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
        return self.produce_list(m_list, **kwargs)

    # Helpers

    def get_key_value_schema_tuple(self, **kwargs):
        """Resolve (key_schema, value_schema, key_schema_id, value_schema_id) from kwargs.

        Args:
            **kwargs: optionally containing key_schema, value_schema, key_schema_id, value_schema_id

        Returns:
            tuple: (key_schema, value_schema, key_schema_id, value_schema_id)"""
        key_schema_str_or_dict = kwargs["key_schema"] if "key_schema" in kwargs else None
        value_schema_str_or_dict = kwargs["value_schema"] if "value_schema" in kwargs else None
        #
        key_schema_id_int = kwargs["key_schema_id"] if "key_schema_id" in kwargs else None
        value_schema_id_int = kwargs["value_schema_id"] if "value_schema_id" in kwargs else None
        #
        return (key_schema_str_or_dict, value_schema_str_or_dict, key_schema_id_int, value_schema_id_int)
