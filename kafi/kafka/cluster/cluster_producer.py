from confluent_kafka import Producer

from kafi.kafka.kafka_producer import KafkaProducer

# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1

#

class ClusterProducer(KafkaProducer):
    def __init__(self, cluster_obj, topic, **kwargs):
        super().__init__(cluster_obj, topic, **kwargs)
        #
        self.on_delivery_function = kwargs["on_delivery"] if "on_delivery" in kwargs else None
        #
        self.producer = Producer(cluster_obj.kafka_config_dict)

    def __del__(self):
        self.flush()

    #

    def close(self):
        self.flush()
        return self.topic_str

    #

    def flush(self):
        self.producer.flush(self.storage_obj.flush_timeout())
        #
        return self.topic_str

    def produce(self, value, **kwargs):
        key = kwargs["key"] if "key" in kwargs else None
        partition = kwargs["partition"] if "partition" in kwargs and kwargs["partition"] is not None else RD_KAFKA_PARTITION_UA
        timestamp = kwargs["timestamp"] if "timestamp" in kwargs and kwargs["timestamp"] is not None else CURRENT_TIME
        headers = kwargs["headers"] if "headers" in kwargs else None
        #
        value_list = value if isinstance(value, list) else [value]
        #
        key_list = key if isinstance(key, list) else [key for _ in value_list]
        #
        if self.keep_partitions_bool:
            partition_int_list = partition if isinstance(partition, list) else [partition for _ in value_list]
        else:
            partition_int_list = [RD_KAFKA_PARTITION_UA for _ in value_list]
        if self.keep_timestamps_bool:
            timestamp_list = timestamp if isinstance(timestamp, list) else [timestamp for _ in value_list]
        else:
            timestamp_list = [CURRENT_TIME for _ in value_list]
        headers_list = headers if isinstance(headers, list) and len(headers) == len(value_list) else [headers for _ in value_list]
        headers_str_bytes_tuple_list_list = [self.storage_obj.headers_to_headers_str_bytes_tuple_list(headers) for headers in headers_list]
        #
        key_bytes_list = []
        value_bytes_list = []
        for value, key, partition_int, timestamp, headers_str_bytes_tuple_list in zip(value_list, key_list, partition_int_list, timestamp_list, headers_str_bytes_tuple_list_list):
            key_str_or_bytes = self.serialize(key, True)
            value_str_or_bytes = self.serialize(value, False)
            #
            timestamp_int = timestamp[1] if isinstance(timestamp, tuple) else timestamp
            #
            self.producer.produce(self.topic_str, value_str_or_bytes, key_str_or_bytes, partition=partition_int, timestamp=timestamp_int, headers=headers_str_bytes_tuple_list, on_delivery=self.on_delivery_function)
            #
            self.written_counter_int += 1
            #
            key_bytes_list.append(key_str_or_bytes)
            value_bytes_list.append(value_str_or_bytes)
        #
        return key_bytes_list, value_bytes_list
