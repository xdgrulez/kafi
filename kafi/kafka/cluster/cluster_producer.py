from confluent_kafka import Producer, TopicPartition

from kafi.kafka.kafka_producer import KafkaProducer

from kafi.helpers import get_millis

# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1

#

class ClusterProducer(KafkaProducer):
    def __init__(self, cluster_obj, topic, **kwargs):
        # The default partitioner function for confluent-kafka is None.
        self.partitioner_fun = kwargs["partitioner_fun"] if "partitioner_fun" in kwargs else None
        #
        super().__init__(cluster_obj, topic, **kwargs)
        #
        def on_delivery(kafka_error, _):
            if kafka_error is not None:
                raise Exception(kafka_error)
        self.on_delivery_fun = kwargs["on_delivery"] if "on_delivery" in kwargs else on_delivery
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
        counter_int = 0
        for message_dict in message_dict_list:
            timestamp = message_dict["timestamp"]
            timestamp_int = timestamp[1] if isinstance(timestamp, tuple) else timestamp
            #
            partition_int = message_dict["partition"]
            if partition_int == RD_KAFKA_PARTITION_UA:
                if self.partitioner_fun is not None:
                    # Use the custom partitioner function for the partitioning.
                    partition_int = self.partitioner_fun(message_dict, counter_int, self.partitions_int, self.projection_fun)
                else:
                    # Let confluent-kafka Proxy do the partitioning if no custom partitioner function is specified.
                    pass
            #
            self.producer.produce(self.topic_str, message_dict["value"], message_dict["key"], partition=partition_int, timestamp=timestamp_int, headers=message_dict["headers"], on_delivery=self.on_delivery_fun)
            self.producer.poll(0)
            #
            self.written_counter_int += 1
        #
        if flush_bool:
            self.flush()
        #
        return self.written_counter_int

    #

    def init_transactions(self, **kwargs):
        timeout_float = kwargs["timeout"] if "timeout" in kwargs else -1.0
        #
        self.producer.init_transactions(timeout_float)
        #
        return self.topic_str

    def begin_transaction(self, **kwargs):
        self.producer.begin_transaction()
        #
        return self.topic_str

    def commit_transaction(self, **kwargs):
        timeout_float = kwargs["timeout"] if "timeout" in kwargs else -1.0
        #
        self.producer.commit_transaction(timeout_float)
        #
        return self.topic_str

    def abort_transaction(self, **kwargs):
        timeout_float = kwargs["timeout"] if "timeout" in kwargs else -1.0
        #
        self.producer.abort_transaction(timeout_float)
        #
        return self.topic_str

    def send_offsets_to_transaction(self, consumer, offsets, **kwargs):
        timeout_float = kwargs["timeout"] if "timeout" in kwargs else -1.0
        #
        str_or_int = list(offsets.keys())[0]
        if isinstance(str_or_int, str):
            topic_str_offsets_dict_dict = offsets
        elif isinstance(str_or_int, int):
            topic_str_offsets_dict_dict = {self.topic_str: offsets}
        #
        offsets_topicPartition_list = [TopicPartition(topic_str, partition_int, offset_int) for topic_str, offsets in topic_str_offsets_dict_dict.items() for partition_int, offset_int in offsets.items()]
        #
        self.producer.send_offsets_to_transaction(offsets_topicPartition_list, consumer.consumer_group_metadata(), timeout_float)
        #
        return topic_str_offsets_dict_dict
