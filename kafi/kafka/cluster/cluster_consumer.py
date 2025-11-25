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
        self.consumer_config_dict.update(cluster_obj.kafka_config_dict)
        #
        if "group.id" not in self.consumer_config_dict:
            self.consumer_config_dict["group.id"] = self.group_str
        if "session.timeout.ms" not in self.consumer_config_dict:
            self.consumer_config_dict["session.timeout.ms"] = cluster_obj.session_timeout_ms()
        if "enable.auto.commit" not in self.consumer_config_dict:
            self.consumer_config_dict["enable.auto.commit"] = self.enable_auto_commit_bool
        #
        self.consumer = Consumer(self.consumer_config_dict)
        #
        list_any_tuple = self.subscribe()
        if cluster_obj.verbose() > 0:
            print(list_any_tuple)

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
            if self.topic_str_partition_int_list_dict is not None:
                print(self.topic_str_partition_int_list_dict)
                topicPartition_list = [topicPartition for topicPartition in partitions if topicPartition.partition in self.topic_str_partition_int_list_dict[topicPartition.topic]]
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

    def assign(self, topic_str_partition_int_list_dict):
        topicPartition_list = topic_str_partition_int_list_dict_to_TopicPartition_list(topic_str_partition_int_list_dict)
        #
        self.consumer.assign(topicPartition_list)

    def seek(self, topic_str, partition_int, offset_int):
        topicPartition = TopicPartition(topic_str, partition_int, offset_int)
        #
        self.consumer.seek(topicPartition)

    #

    def consume_impl(self, **kwargs):
        n_int = kwargs["n"] if "n" in kwargs and kwargs["n"] != ALL_MESSAGES else 1
        #
        message_list = self.consumer.consume(n_int, self.storage_obj.consume_timeout())
        #
        message_dict_list = []
        for message in message_list:
            if message.error() is None:
                message_dict = {"topic": message.topic(),
                                "headers": message.headers(),
                                "partition": message.partition(),
                                "offset": message.offset(),
                                "timestamp": message.timestamp(),
                                "key": message.key(),
                                "value": message.value()}
            else:
                raise Exception(f"Error consuming topic(s) {self.topic_str_list}: {message.error().str()}, topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}")
            message_dict_list.append(message_dict)
        #
        return message_dict_list

    #

    def commit(self, offsets=None, **kwargs):
        asynchronous_bool = kwargs["asynchronous"] if "asynchronous" in kwargs else False
        #
        if offsets is not None:
            str_or_int = list(offsets.keys())[0]
            if isinstance(str_or_int, str):
                topic_str_offsets_dict_dict = offsets
            elif isinstance(str_or_int, int):
                topic_str_offsets_dict_dict = {topic_str: offsets for topic_str in self.topic_str_list}
            #
            offsets_topicPartition_list = [TopicPartition(topic_str, partition_int, offset_int) for topic_str, offsets in topic_str_offsets_dict_dict.items() for partition_int, offset_int in offsets.items()]
            #
            commit_topicPartition_list = self.consumer.commit(offsets=offsets_topicPartition_list, asynchronous=asynchronous_bool)
            #
            topic_str_offsets_dict_dict = topicPartition_list_to_offsets_dict(commit_topicPartition_list)
        else:
            self.consumer.commit(asynchronous=asynchronous_bool)
            topic_str_offsets_dict_dict = {}
        #
        return topic_str_offsets_dict_dict

    def offsets(self, **kwargs):
        timeout_float = kwargs["timeout"] if "timeout" in kwargs else -1.0
        #
        assignment_topicPartition_list = self.consumer.assignment()
        committed_topicPartition_list = self.consumer.committed(assignment_topicPartition_list, timeout=timeout_float)
        #
        topic_str_offsets_dict_dict = topicPartition_list_to_offsets_dict(committed_topicPartition_list)
        #
        return topic_str_offsets_dict_dict

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


def topic_str_partition_int_list_dict_to_TopicPartition_list(topic_str_partition_int_dict):
    topicPartition_list = []
    for topic_str, partition_int_list in topic_str_partition_int_dict.items():
        for partition_int in partition_int_list:
            topicPartition_list.append(TopicPartition(topic_str, partition_int))
    #
    return topicPartition_list
