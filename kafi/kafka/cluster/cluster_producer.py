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
        def on_delivery(kafka_error, _):
            if kafka_error is not None:
                raise Exception(kafka_error)
        self.on_delivery_function = kwargs["on_delivery"] if "on_delivery" in kwargs else on_delivery
        #
        # Producer config
        #
        producer_config_dict = cluster_obj.kafka_config_dict.copy()
        #
        if "config" in kwargs:
            for key_str, value in kwargs["config"].items():
                producer_config_dict[key_str] = value
        #
        self.producer = Producer(producer_config_dict)
        #
        # The default partitioner function for confluent-kafka is None.
        self.partitioner_function = kwargs["partitioner_function"] if "partitioner_function" in kwargs else None


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

    #

    def produce_impl(self, message_dict_list, **kwargs):
        flush_bool = kwargs["flush"] if "flush" in kwargs else False
        #
        for counter_int, message_dict in zip(range(len(message_dict_list)), message_dict_list):
            timestamp = message_dict["timestamp"]
            timestamp_int = timestamp[1] if isinstance(timestamp, tuple) else timestamp
            #
            partition_int = message_dict["partition"]
            if partition_int == RD_KAFKA_PARTITION_UA:
                if self.partitioner_function is not None:
                    # Use the custom partitioner function for the partitioning.
                    partition_int = self.partitioner_function(message_dict, counter_int, self.partitions_int, self.projection_function)
                else:
                    # Let confluent-kafka Proxy do the partitioning if no custom partitioner function is specified.
                    pass
            #
            self.producer.produce(self.topic_str, message_dict["value"], message_dict["key"], partition=partition_int, timestamp=timestamp_int, headers=message_dict["headers"], on_delivery=self.on_delivery_function)
            #
            self.written_counter_int += 1
        #
        if flush_bool:
            self.flush()
        #
        return self.written_counter_int
