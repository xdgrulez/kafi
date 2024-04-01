from confluent_kafka import Consumer, TopicPartition

from kafi.kafka.kafka_consumer import KafkaConsumer

# Constants

ALL_MESSAGES = -1

#

class ClusterConsumer(KafkaConsumer):
    def __init__(self, cluster_obj, *topics, **kwargs):
        super().__init__(cluster_obj, *topics, **kwargs)
        #
        # Consumer config
        #
        consumer_config_dict = cluster_obj.kafka_config_dict.copy()
        #
        self.consumer_config_dict["group.id"] = self.group_str
        self.consumer_config_dict["session.timeout.ms"] = cluster_obj.session_timeout_ms()
        self.consumer_config_dict["enable.auto.commit"] = self.enable_auto_commit_bool        
        #
        consumer_config_key_str_list = ["group.id", "group.instance.id", "partition.assignment.strategy", "session.timeout.ms", "heartbeat.interval.ms", "group.protocol.type", "coordinator.query.interval.ms", "max.poll.interval.ms", "enable.auto.commit", "auto.commit.interval.ms", "enable.auto.offset.store", "queued.min.messages", "queued.max.messages.kbytes", "fetch.wait.max.ms", "fetch.queue.backoff.ms", "fetch.message.max.bytes", "max.partition.fetch.bytes", "fetch.max.bytes", "fetch.min.bytes", "fetch.error.backoff.ms", "offset.store.method", "isolation.level", "enable.partition.eof", "check.crcs"]
        for consumer_config_key_str in consumer_config_key_str_list:
            if consumer_config_key_str in kwargs:
                consumer_config_dict[consumer_config_key_str] = kwargs[consumer_config_key_str]
        #
        self.producer = Consumer(consumer_config_dict)
        #
        self.subscribe()

    def __del__(self):
        self.close()

    #

    def subscribe(self):
        def on_assign(consumer, partitions):
            def set_offset(topicPartition):
                if topicPartition.topic in self.topic_str_start_offsets_dict_dict:
                    offsets = self.topic_str_start_offsets_dict_dict[topicPartition.topic]
                    if topicPartition.partition in offsets:
                        offset_int = offsets[topicPartition.partition]
                        topicPartition.offset = offset_int
                return topicPartition
            #
            if self.topic_str_start_offsets_dict_dict is not None:
                topicPartition_list = [set_offset(topicPartition) for topicPartition in partitions]
                consumer.assign(topicPartition_list)
        self.consumer.subscribe(self.topic_str_list, on_assign=on_assign)
        #
        return self.topic_str_list, self.group_str
    
    def unsubscribe(self):
        self.consumer.unsubscribe()
        #
        return self.topic_str_list, self.group_str

    def close(self):
        self.consumer.close()
        #
        return self.topic_str_list, self.group_str

    #

    def consume_impl(self, **kwargs):
        n_int = kwargs["n"] if "n" in kwargs and kwargs["n"] != ALL_MESSAGES else 1
        #
        message_list = self.consumer.consume(n_int, self.storage_obj.consume_timeout())
        #
        deserialized_message_dict_list = [{"topic": message.topic(), "headers": message.headers(), "partition": message.partition(), "offset": message.offset(), "timestamp": message.timestamp(), "key": self.deserialize(message.key(), self.topic_str_key_type_str_dict[message.topic()]), "value": self.deserialize(message.value(), self.topic_str_value_type_str_dict[message.topic()])} for message in message_list]
        #
        return deserialized_message_dict_list

    #

    def commit(self, offsets=None, **kwargs):
        asynchronous_bool = kwargs["asynchronous"] if "asynchronous" in kwargs else False
        #
        if offsets is not None:
            str_or_int = list(offsets.keys())[0]
            if isinstance(str_or_int, str):
                offsets_dict = offsets
            elif isinstance(str_or_int, int):
                offsets_dict = {topic_str: offsets for topic_str in self.topic_str_list}
            #
            offsets_topicPartition_list = [TopicPartition(topic_str, partition_int, offset_int) for topic_str, offsets in offsets_dict.items() for partition_int, offset_int in offsets.items()]
            #
            commit_topicPartition_list = self.consumer.commit(offsets=offsets_topicPartition_list, asynchronous=asynchronous_bool)
            #
            offsets_dict = topicPartition_list_to_offsets_dict(commit_topicPartition_list)
        else:
            self.consumer.commit()
            offsets_dict = {}
        #
        return offsets_dict

    def offsets(self, **kwargs):
        timeout_float = kwargs["timeout"] if "timeout" in kwargs else -1.0
        #
        assignment_topicPartition_list = self.consumer.assignment()
        committed_topicPartition_list = self.consumer.committed(assignment_topicPartition_list, timeout=timeout_float)
        #
        offsets_dict = topicPartition_list_to_offsets_dict(committed_topicPartition_list)
        #
        return offsets_dict

    def memberid(self):
        member_id_str = self.consumer.memberid()
        #
        return member_id_str 

#

def topicPartition_list_to_offsets_dict(topicPartition_list):
    offsets_dict = {}
    for topicPartition in topicPartition_list:
        topic_str = topicPartition.topic
        partition_int = topicPartition.partition
        offset_int = topicPartition.offset
        #
        if topic_str in offsets_dict:
            offsets = offsets_dict[topic_str]
        else:
            offsets = {}
        offsets[partition_int] = offset_int
        offsets_dict[topic_str] = offsets
    #
    return offsets_dict
