from fnmatch import fnmatch
import time

from kafi.helpers import pattern_match
from kafi.kafka.kafka_admin import KafkaAdmin

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AclBinding, AclBindingFilter, AclOperation, AclPermissionType, AdminClient, ConfigResource, _ConsumerGroupState, _ConsumerGroupTopicPartitions, NewPartitions, NewTopic, ResourcePatternType, ResourceType

#

OFFSET_END = -1

#

class ClusterAdmin(KafkaAdmin):
    def __init__(self, cluster_obj, **kwargs):
        super().__init__(cluster_obj, **kwargs)
        #
        self.adminClient = AdminClient(cluster_obj.kafka_config_dict)

    # ACLs

    def acls(self, name=None, restype="any", resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        resourceType = str_to_resourceType(restype)
        name_str = name
        resourcePatternType = str_to_resourcePatternType(resource_pattern_type)
        principal_str = principal
        host_str = host
        aclOperation = str_to_aclOperation(operation)
        aclPermissionType = str_to_aclPermissionType(permission_type)
        #
        aclBindingFilter = AclBindingFilter(resourceType, name_str, resourcePatternType, principal_str, host_str, aclOperation, aclPermissionType)
        aclBinding_list = self.adminClient.describe_acls(aclBindingFilter).result()
        #
        return [aclBinding_to_dict(aclBinding) for aclBinding in aclBinding_list]

    def create_acl(self, name=None, restype="any", resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        resourceType = str_to_resourceType(restype)
        name_str = name
        resourcePatternType = str_to_resourcePatternType(resource_pattern_type)
        principal_str = principal
        host_str = host
        aclOperation = str_to_aclOperation(operation)
        aclPermissionType = str_to_aclPermissionType(permission_type)
        #
        aclBinding = AclBinding(resourceType, name_str, resourcePatternType, principal_str, host_str, aclOperation, aclPermissionType)
        self.adminClient.create_acls([aclBinding])[aclBinding].result()
        #
        return aclBinding_to_dict(aclBinding)

    def delete_acl(self, name=None, restype="any", resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        resourceType = str_to_resourceType(restype)
        name_str = name
        resourcePatternType = str_to_resourcePatternType(resource_pattern_type)
        principal_str = principal
        host_str = host
        aclOperation = str_to_aclOperation(operation)
        aclPermissionType = str_to_aclPermissionType(permission_type)
        #
        aclBindingFilter = AclBindingFilter(resourceType, name_str, resourcePatternType, principal_str, host_str, aclOperation, aclPermissionType)
        aclBinding_list = self.adminClient.delete_acls([aclBindingFilter])[aclBindingFilter].result()
        #
        return [aclBinding_to_dict(aclBinding) for aclBinding in aclBinding_list]

    # Brokers

    def brokers(self, pattern=None):
        pattern_int_or_str_list = pattern if isinstance(pattern, list) else [pattern]
        #
        if pattern_int_or_str_list == [None]:
            pattern_str_list = ["*"]
        else:
            pattern_str_list = [str(pattern_int_or_str) for pattern_int_or_str in pattern_int_or_str_list]
        #
        broker_dict = {broker_int: brokerMetadata.host + ":" + str(brokerMetadata.port) for broker_int, brokerMetadata in self.adminClient.list_topics().brokers.items() if any(fnmatch(str(broker_int), pattern_str) for pattern_str in pattern_str_list)}
        #
        return broker_dict

    def broker_config(self, pattern=None, config=None, **kwargs):
        config_dict = config
        test_bool = kwargs["test"] if "test" in kwargs else False
        #
        broker_dict = self.brokers(pattern)
        #
        if config is not None:
            for broker_int in broker_dict:
                self.set_resource_config_dict(ResourceType.BROKER, str(broker_int), config_dict, test=test_bool)
        #
        broker_int_broker_config_dict = {broker_int: self.get_resource_config_dict(ResourceType.BROKER, str(broker_int)) for broker_int in broker_dict}
        #
        return broker_int_broker_config_dict

    # Broker/Topic Configuration

    def get_resource_config_dict(self, resourceType, resource_str):
        configResource = ConfigResource(resourceType, resource_str)
        # configEntry_dict: ConfigResource -> ConfigEntry
        configEntry_dict = self.adminClient.describe_configs([configResource])[configResource].result()
        # config_dict: str -> str
        config_dict = {config_key_str: configEntry.value for config_key_str, configEntry in configEntry_dict.items()}
        return config_dict

    def set_resource_config_dict(self, resourceType, resource_str, new_config_dict, **kwargs):
        test_bool = kwargs["test"] if "test" in kwargs else False
        #
        old_config_dict = self.get_resource_config_dict(resourceType, resource_str)
        #
        if resourceType == ResourceType.BROKER:
            # https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html#cp-config-brokers
            white_list_key_str_list = ["advertised.listeners", "background.threads", "compression.type", "confluent.balancer.enable", "confluent.balancer.heal.uneven.load.trigger", "confluent.balancer.throttle.bytes.per.second", "confluent.tier.local.hotset.bytes", "confluent.tier.local.hotset.ms", "listeners", "log.flush.interval.messages", "log.flush.interval.ms", "log.retention.bytes", "log.retention.ms", "log.roll.jitter.ms", "log.roll.ms", "log.segment.bytes", "log.segment.delete.delay.ms", "message.max.bytes", "min.insync.replicas", "num.io.threads", "num.network.threads", "num.recovery.threads.per.data.dir", "num.replica.fetchers", "unclean.leader.election.enable", "confluent.balancer.exclude.topic.names", "confluent.balancer.exclude.topic.prefixes", "confluent.clm.enabled", "confluent.clm.frequency.in.hours", "confluent.clm.max.backup.days", "confluent.clm.min.delay.in.minutes", "confluent.clm.topic.retention.days.to.backup.days", "confluent.cluster.link.fetch.response.min.bytes", "confluent.cluster.link.fetch.response.total.bytes", "confluent.cluster.link.io.max.bytes.per.second", "confluent.tier.enable", "confluent.tier.max.partition.fetch.bytes.override", "log.cleaner.backoff.ms", "log.cleaner.dedupe.buffer.size", "log.cleaner.delete.retention.ms", "log.cleaner.io.buffer.load.factor", "log.cleaner.io.buffer.size", "log.cleaner.io.max.bytes.per.second", "log.cleaner.max.compaction.lag.ms", "log.cleaner.min.cleanable.ratio", "log.cleaner.min.compaction.lag.ms", "log.cleaner.threads", "log.cleanup.policy", "log.deletion.max.segments.per.run", "log.index.interval.bytes", "log.index.size.max.bytes", "log.message.timestamp.difference.max.ms", "log.message.timestamp.type", "log.preallocate", "max.connection.creation.rate", "max.connections", "max.connections.per.ip", "max.connections.per.ip.overrides", "principal.builder.class", "sasl.enabled.mechanisms", "sasl.jaas.config", "sasl.kerberos.kinit.cmd", "sasl.kerberos.min.time.before.relogin", "sasl.kerberos.principal.to.local.rules", "sasl.kerberos.service.name", "sasl.kerberos.ticket.renew.jitter", "sasl.kerberos.ticket.renew.window.factor", "sasl.login.refresh.buffer.seconds", "sasl.login.refresh.min.period.seconds", "sasl.login.refresh.window.factor", "sasl.login.refresh.window.jitter", "sasl.mechanism.inter.broker.protocol", "ssl.cipher.suites", "ssl.client.auth", "ssl.enabled.protocols", "ssl.keymanager.algorithm", "ssl.protocol", "ssl.provider", "ssl.trustmanager.algorithm", "confluent.cluster.link.replication.quota.mode", "confluent.metadata.server.cluster.registry.clusters", "confluent.reporters.telemetry.auto.enable", "confluent.security.event.router.config", "confluent.telemetry.enabled", "confluent.tier.topic.delete.backoff.ms", "confluent.tier.topic.delete.check.interval.ms", "confluent.tier.topic.delete.max.inprogress.partitions", "follower.replication.throttled.rate", "follower.replication.throttled.replicas", "leader.replication.throttled.rate", "leader.replication.throttled.replicas", "listener.security.protocol.map", "log.message.downconversion.enable", "metric.reporters", "ssl.endpoint.identification.algorithm", "ssl.engine.factory.class", "ssl.secure.random.implementation"]
        #
        alter_config_dict = {}
        for key_str, value_str in old_config_dict.items():
            if resourceType == ResourceType.BROKER:
                if key_str not in white_list_key_str_list:
                    continue
            if key_str in new_config_dict:
                value_str = new_config_dict[key_str]
            if value_str:
                alter_config_dict[key_str] = value_str
        #
        alter_configResource = ConfigResource(resourceType, resource_str, set_config=alter_config_dict)
        #
        future = self.adminClient.alter_configs([alter_configResource], validate_only=test_bool)[alter_configResource]
        #
        future.result()

    # Groups

    def delete_groups(self, pattern, state_pattern="*"):
        pattern_str_or_str_list = pattern
        #
        group_str_list = self.groups(pattern_str_or_str_list, state_pattern)
        if not group_str_list:
            return []
        #
        group_str_group_future_dict = self.adminClient.delete_consumer_groups(group_str_list)
        for group_future in group_str_group_future_dict.values():
            group_future.result() 
        group_str_list = list(group_str_group_future_dict.keys())
        #
        return group_str_list

    def describe_groups(self, pattern="*", state_pattern="*"):
        group_str_list = self.groups(pattern, state_pattern)
        if not group_str_list:
            return {}
        #
        group_str_consumerGroupDescription_future_dict = self.adminClient.describe_consumer_groups(group_str_list)
        group_str_group_description_dict_dict = {group_str: consumerGroupDescription_to_group_description_dict(consumerGroupDescription_future.result()) for group_str, consumerGroupDescription_future in group_str_consumerGroupDescription_future_dict.items()}
        #
        return group_str_group_description_dict_dict

    def groups(self, pattern="*", state_pattern="*", state=False):
        pattern_str_or_str_list = [pattern] if isinstance(pattern, str) else pattern
        #
        consumerGroupState_pattern_str_list = [state_pattern] if isinstance(state_pattern, str) else state_pattern
        consumerGroupState_set = set([str_to_consumerGroupState(consumerGroupState_str) for consumerGroupState_str in all_consumerGroupState_str_list if any(fnmatch(consumerGroupState_str, consumerGroupState_pattern_str) for consumerGroupState_pattern_str in consumerGroupState_pattern_str_list)])
        if not consumerGroupState_set:
            return {} if state else []
        #
        listConsumerGroupsResult = self.adminClient.list_consumer_groups(states=consumerGroupState_set).result()
        consumerGroupListing_list = listConsumerGroupsResult.valid
        #
        group_str_state_str_dict = {consumerGroupListing.group_id: consumerGroupState_to_str(consumerGroupListing.state) for consumerGroupListing in consumerGroupListing_list if any(fnmatch(consumerGroupListing.group_id, pattern_str) for pattern_str in pattern_str_or_str_list)}
        #
        return group_str_state_str_dict if state else list(group_str_state_str_dict.keys())

    def group_offsets(self, pattern, group_offsets=None, state_pattern="*"):
        group_offsets_dict = group_offsets
        #
        if group_offsets_dict is not None:
            group_str = pattern
            #
            consumerGroupTopicPartitions = group_str_group_offsets_to_consumerGroupTopicPartitions(group_str, group_offsets)
            group_str_consumerGroupTopicPartitions_future_dict = self.adminClient.alter_consumer_group_offsets([consumerGroupTopicPartitions])
            #
            group_offsets = group_str_consumerGroupTopicPartitions_future_dict_to_group_offsets(group_str_consumerGroupTopicPartitions_future_dict)
        else:
            group_str_list = self.groups(pattern, state_pattern)
            if not group_str_list:
                return {}
            #
            consumerGroupTopicPartitions_list = [_ConsumerGroupTopicPartitions(group_str) for group_str in group_str_list]
            group_str_consumerGroupTopicPartitions_future_dict = self.adminClient.list_consumer_group_offsets(consumerGroupTopicPartitions_list)
            #
            group_offsets = group_str_consumerGroupTopicPartitions_future_dict_to_group_offsets(group_str_consumerGroupTopicPartitions_future_dict)
        #
        return group_offsets

    # Topics

    def block_topic(self, topic, exists=True):
        topic_str = topic
        exists_bool = exists
        #
        def exists(topic_str):
            return self.list_topics(topic_str) != []
        
        num_retries_int = 0
        while True:
            if exists_bool:
                if exists(topic_str):
                    return True
            else:
                if not exists(topic_str):
                    return True
            #
            num_retries_int += 1
            if num_retries_int >= self.storage_obj.block_num_retries():
                raise Exception(f"Retried waiting for topic {'deletion' if exists else 'creation'} {self.storage_obj.block_num_retries()} times, stopping.")
            #
            time.sleep(self.storage_obj.block_interval())

    #

    def config(self, pattern, config=None, **kwargs):
        config_dict = config
        test_bool = kwargs["test"] if "test" in kwargs else False
        #
        topic_str_list = self.list_topics(pattern)
        #
        if config is not None:
            for topic_str in topic_str_list:
                self.set_resource_config_dict(ResourceType.TOPIC, topic_str, config_dict, test=test_bool)
        #
        topic_str_config_dict_dict = {topic_str: self.get_resource_config_dict(ResourceType.TOPIC, topic_str) for topic_str in topic_str_list}
        #
        return topic_str_config_dict_dict

    #

    def create(self, topic, partitions=1, replication=-1, config={}, **kwargs):
        topic_str = topic
        partitions_int = partitions
        replication_int = replication
        config_dict = config
        block_bool = kwargs["block"] if "block" in kwargs else True
        #
        config_dict["retention.ms"] = config_dict["retention.ms"] if "retention.ms" in config_dict else self.storage_obj.retention_ms()
        #
        newTopic = NewTopic(topic_str, partitions_int, replication_factor=replication_int, config=config_dict)
        self.adminClient.create_topics([newTopic])
        #
        if block_bool:
            self.block_topic(topic_str, exists=True)
        #
        return topic_str

    def delete(self, pattern, **kwargs):
        pattern_str_or_str_list = pattern
        block_bool = kwargs["block"] if "block" in kwargs else True
        #
        topic_str_list = self.list_topics(pattern_str_or_str_list)
        #
        if topic_str_list:
            self.adminClient.delete_topics(topic_str_list)
            if block_bool:
                for topic_str in topic_str_list:
                    self.block_topic(topic_str, exists=False)
        #
        return topic_str_list

    #

    def list_topics(self, pattern=None):
        topic_str_list = list(self.adminClient.list_topics().topics.keys())
        #
        filtered_topic_str_list = pattern_match(topic_str_list, pattern)
        #
        return filtered_topic_str_list

    def offsets_for_times(self, pattern, partitions_timestamps, replace_not_found=False, **kwargs):
        replace_not_found_bool = replace_not_found
        #
        timeout_float = kwargs["timeout"] if "timeout" in kwargs else -1.0
        #
        topic_str_list = self.list_topics(pattern)
        #
        topic_str_partition_int_timestamp_int_dict_dict = self.get_topic_str_partition_int_timestamp_int_dict_dict(topic_str_list, partitions_timestamps)
        #
        topic_str_offsets_dict_dict = {}
        for topic_str in topic_str_list:
            offsets_dict = {}
            #
            topicPartition_list = [TopicPartition(topic_str, partition_int, timestamp_int) for partition_int, timestamp_int in topic_str_partition_int_timestamp_int_dict_dict[topic_str].items()]
            if topicPartition_list:
                config_dict = self.storage_obj.kafka_config_dict.copy()
                config_dict["group.id"] = "dummy_group_id"
                consumer = Consumer(config_dict)
                topicPartition_list1 = consumer.offsets_for_times(topicPartition_list, timeout=timeout_float)
                #
                for topicPartition in topicPartition_list1:
                    offsets_dict[topicPartition.partition] = topicPartition.offset
                #
                topic_str_offsets_dict_dict[topic_str] = offsets_dict
        #
        if replace_not_found_bool:
            topic_str_offsets_dict_dict = self.replace_not_found(topic_str_offsets_dict_dict)
        #
        return topic_str_offsets_dict_dict

    def partitions(self, pattern=None, partitions=None, verbose=False, **kwargs):
        pattern_str_or_str_list = pattern
        partitions_int = partitions
        verbose_bool = verbose
        test_bool = kwargs["test"] if "test" in kwargs else False
        #
        if partitions_int is not None:
            topic_str_list = self.list_topics(pattern_str_or_str_list)
            #
            newPartitions_list = [NewPartitions(topic_str, partitions_int) for topic_str in topic_str_list]
            topic_str_future_dict = self.adminClient.create_partitions(newPartitions_list, validate_only=test_bool)
            #
            for future in topic_str_future_dict.values():
                future.result()
        #
        if pattern_str_or_str_list is None:
            pattern_str_or_str_list = ["*"]
        elif isinstance(pattern_str_or_str_list, str):
            pattern_str_or_str_list = [pattern_str_or_str_list]
        #
        topic_str_topicMetadata_dict = self.adminClient.list_topics().topics
        #
        if verbose_bool:
            topic_str_topic_dict_dict = {topic_str: topicMetadata_to_topic_dict(topic_str_topicMetadata_dict[topic_str]) for topic_str in topic_str_topicMetadata_dict if any(fnmatch(topic_str, pattern_str) for pattern_str in pattern_str_or_str_list)}
            #
            topic_str_partition_int_partition_dict_dict_dict = {}
            for topic_str, topic_dict in topic_str_topic_dict_dict.items():
                partitions_dict = topic_dict["partitions"]
                topic_str_partition_int_partition_dict_dict_dict[topic_str] = partitions_dict
            #
            return topic_str_partition_int_partition_dict_dict_dict
        else:
            topic_str_partitions_int_dict = {topic_str: len(topic_str_topicMetadata_dict[topic_str].partitions) for topic_str in topic_str_topicMetadata_dict if any(fnmatch(topic_str, pattern_str) for pattern_str in pattern_str_or_str_list)}
            #
            return topic_str_partitions_int_dict

    def watermarks(self, pattern, **kwargs):
        timeout_float = kwargs["timeout"] if "timeout" in kwargs else -1.0
        #
        config_dict = self.storage_obj.kafka_config_dict.copy()
        config_dict["group.id"] = "dummy_group_id"
        consumer = Consumer(config_dict)
        #
        topic_str_list = self.list_topics(pattern)
        topic_str_partition_int_offsets_tuple_dict_dict = {}
        for topic_str in topic_str_list:
            partitions_int = self.partitions(topic_str)[topic_str]
            partition_int_offsets_tuple_dict = {partition_int: consumer.get_watermark_offsets(TopicPartition(topic_str, partition=partition_int), timeout_float) for partition_int in range(partitions_int)}
            topic_str_partition_int_offsets_tuple_dict_dict[topic_str] = partition_int_offsets_tuple_dict
        consumer.close()
        return topic_str_partition_int_offsets_tuple_dict_dict

    def delete_records(self, pattern_or_offsets, **kwargs):
        request_timeout_float = kwargs["request_timeout"] if "request_timeout" in kwargs else None
        operation_timeout_float = kwargs["operation_timeout"] if "operation_timeout" in kwargs else None
        #
        topicPartition_list = []
        if isinstance(pattern_or_offsets, dict):
            topic_str_offsets_dict_dict = pattern_or_offsets
            topicPartition_list = [TopicPartition(topic_str, partition_int, offset_int) for topic_str, offsets_dict in topic_str_offsets_dict_dict.items() for partition_int, offset_int in offsets_dict.items()]
        else:
            pattern = pattern_or_offsets
            topic_str_list = self.list_topics(pattern)
            for topic_str in topic_str_list:
                partitions_int = self.partitions(topic_str)[topic_str]
                for partition_int in range(0, partitions_int):
                    topicPartition_list.append(TopicPartition(topic_str, partition_int, OFFSET_END))
        #
        if request_timeout_float is not None and operation_timeout_float is not None:
            self.adminClient.delete_records(topicPartition_list, request_timeout=request_timeout_float, operation_timeout=operation_timeout_float)
        elif request_timeout_float is not None and operation_timeout_float is None:
            self.adminClient.delete_records(topicPartition_list, request_timeout=request_timeout_float)
        elif request_timeout_float is None and operation_timeout_float is not None:
            self.adminClient.delete_records(topicPartition_list, operation_timeout=operation_timeout_float)
        else:
            self.adminClient.delete_records(topicPartition_list)

# helpers

# group_offsets =TA group_str_topic_str_offsets_dict_dict_dict
# topic_offsets =TA topic_str_offsets_dict_dict
# (partition_)offsets =TA offsets_dict
def group_str_consumerGroupTopicPartitions_future_dict_to_consumerGroupTopicPartitions_list(group_str_consumerGroupTopicPartitions_future_dict):
    consumerGroupTopicPartitions_list = [consumerGroupTopicPartitions_future.result() for consumerGroupTopicPartitions_future in group_str_consumerGroupTopicPartitions_future_dict.values()]
    #
    return consumerGroupTopicPartitions_list


def consumerGroupTopicPartitions_list_to_group_offsets(consumerGroupTopicPartitions_list):
    group_str_topic_str_offsets_dict_dict_dict = {}
    for consumerGroupTopicPartitions in consumerGroupTopicPartitions_list:
        group_str = consumerGroupTopicPartitions.group_id
        topic_str_offsets_dict_dict = consumerGroupTopicPartitions_to_topic_offsets(consumerGroupTopicPartitions)
        group_str_topic_str_offsets_dict_dict_dict[group_str] = topic_str_offsets_dict_dict
    #
    return group_str_topic_str_offsets_dict_dict_dict


def consumerGroupTopicPartitions_to_topic_offsets(consumerGroupTopicPartitions):
    topic_str_partition_int_offset_int_tuple_list_dict = {}
    for topicPartition in consumerGroupTopicPartitions.topic_partitions:
        topic_str = topicPartition.topic
        partition_int_offset_int_tuple = (topicPartition.partition, topicPartition.offset)
        if topic_str in topic_str_partition_int_offset_int_tuple_list_dict:
            topic_str_partition_int_offset_int_tuple_list_dict[topic_str].append(partition_int_offset_int_tuple)
        else:
            topic_str_partition_int_offset_int_tuple_list_dict[topic_str] = [(topicPartition.partition, topicPartition.offset)]
    #
    topic_str_offsets_dict_dict = {topic_str: {partition_int_offset_int_tuple[0]: partition_int_offset_int_tuple[1] for partition_int_offset_int_tuple in partition_int_offset_int_tuple_list} for topic_str, partition_int_offset_int_tuple_list in topic_str_partition_int_offset_int_tuple_list_dict.items()}
    #
    return topic_str_offsets_dict_dict


def group_str_consumerGroupTopicPartitions_future_dict_to_group_offsets(group_str_consumerGroupTopicPartitions_future_dict):
    consumerGroupTopicPartitions_list = group_str_consumerGroupTopicPartitions_future_dict_to_consumerGroupTopicPartitions_list(group_str_consumerGroupTopicPartitions_future_dict)
    #
    group_offsets = consumerGroupTopicPartitions_list_to_group_offsets(consumerGroupTopicPartitions_list)
    #
    return group_offsets

def group_str_group_offsets_to_consumerGroupTopicPartitions(group_str, group_offsets):
    topicPartition_list = []
    for topic_str, partition_offsets in group_offsets.items():
        for partition_int, offset_int in partition_offsets.items():
            topicPartition_list.append(TopicPartition(topic_str, partition_int, offset_int))
    #
    consumerGroupTopicPartitions = _ConsumerGroupTopicPartitions(group_str, topicPartition_list)
    #
    return consumerGroupTopicPartitions


def consumerGroupDescription_to_group_description_dict(consumerGroupDescription):
    group_description_dict = {"group_id": consumerGroupDescription.group_id,
                              "is_simple_consumer_group": consumerGroupDescription.is_simple_consumer_group,
                              "members": [memberDescription_to_dict(memberDescription) for memberDescription in consumerGroupDescription.members],
                              "partition_assignor": consumerGroupDescription.partition_assignor,
                              "state": consumerGroupState_to_str(consumerGroupDescription.state),
                              "coordinator": node_to_dict(consumerGroupDescription.coordinator)}
    return group_description_dict


def memberDescription_to_dict(memberDescription):
    dict = {"member_id": memberDescription.member_id,
            "client_id": memberDescription.client_id,
            "host": memberDescription.host,
            "assignment": memberAssignment_to_dict(memberDescription.assignment),
            "group_instance_id": memberDescription.group_instance_id}
    return dict


def memberAssignment_to_dict(memberAssignment):
    dict = {"topic_partitions": [topicPartition_to_dict(topicPartition) for topicPartition in memberAssignment.topic_partitions]}
    return dict


def topicPartition_to_dict(topicPartition):
    dict = {"error": kafkaError_to_error_dict(topicPartition.error),
            "metadata": topicPartition.metadata,
            "offset": topicPartition.offset,
            "partition": topicPartition.partition,
            "topic": topicPartition.topic}
    return dict


def node_to_dict(node):
    dict = {"id": node.id,
            "id_string": node.id_string,
            "host": node.host,
            "port": node.port,
            "rack": node.rack}
    return dict


all_consumerGroupState_str_list = ["unknown", "preparing_rebalancing", "completing_rebalancing", "stable", "dead", "empty"]


def consumerGroupState_to_str(consumerGroupState):
    if consumerGroupState == _ConsumerGroupState.UNKOWN:
        return "unknown"
    elif consumerGroupState == _ConsumerGroupState.PREPARING_REBALANCING:
        return "preparing_rebalancing"
    elif consumerGroupState == _ConsumerGroupState.COMPLETING_REBALANCING:
        return "completing_rebalancing"
    elif consumerGroupState == _ConsumerGroupState.STABLE:
        return "stable"
    elif consumerGroupState == _ConsumerGroupState.DEAD:
        return "dead"
    elif consumerGroupState == _ConsumerGroupState.EMPTY:
        return "empty"


def str_to_consumerGroupState(consumerGroupState_str):
    if consumerGroupState_str == "unknown":
        return _ConsumerGroupState.UNKOWN
    elif consumerGroupState_str == "preparing_rebalancing":
        return _ConsumerGroupState.PREPARING_REBALANCING 
    elif consumerGroupState_str == "completing_rebalancing":
        return _ConsumerGroupState.COMPLETING_REBALANCING
    elif consumerGroupState_str == "stable":
        return _ConsumerGroupState.STABLE
    elif consumerGroupState_str == "dead":
        return _ConsumerGroupState.DEAD
    elif consumerGroupState_str == "empty":
        return _ConsumerGroupState.EMPTY


def str_to_resourceType(restype_str):
    restype_str1 = restype_str.lower()
    if restype_str1 == "unknown":
        return ResourceType.UNKNOWN
    elif restype_str1 == "any":
        return ResourceType.ANY
    elif restype_str1 == "topic":
        return ResourceType.TOPIC
    elif restype_str1 == "group":
        return ResourceType.GROUP
    elif restype_str1 == "broker":
        return ResourceType.BROKER


def str_to_resourcePatternType(resource_pattern_type_str):
    resource_pattern_type_str1 = resource_pattern_type_str.lower()
    if resource_pattern_type_str1 == "unknown":
        return ResourcePatternType.UNKNOWN
    elif resource_pattern_type_str1 == "any":
        return ResourcePatternType.ANY
    elif resource_pattern_type_str1 == "match":
        return ResourcePatternType.MATCH
    elif resource_pattern_type_str1 == "literal":
        return ResourcePatternType.LITERAL
    elif resource_pattern_type_str1 == "prefixed":
        return ResourcePatternType.PREFIXED


def resourceType_to_str(resourceType):
    if resourceType == ResourceType.UNKNOWN:
        return "unknown"
    elif resourceType == ResourceType.ANY:
        return "any"
    elif resourceType == ResourceType.TOPIC:
        return "topic"
    elif resourceType == ResourceType.GROUP:
        return "group"
    elif resourceType == ResourceType.BROKER:
        return "broker"
    

def resourcePatternType_to_str(resourcePatternType):
    if resourcePatternType == ResourcePatternType.UNKNOWN:
        return "unknown"
    elif resourcePatternType == ResourcePatternType.ANY:
        return "any"
    elif resourcePatternType == ResourcePatternType.MATCH:
        return "match"
    elif resourcePatternType == ResourcePatternType.LITERAL:
        return "literal"
    elif resourcePatternType == ResourcePatternType.PREFIXED:
        return "prefixed"


def aclOperation_to_str(aclOperation):
    if aclOperation == AclOperation.UNKNOWN:
        return "unknown"
    elif aclOperation == AclOperation.ANY:
        return "any"
    elif aclOperation == AclOperation.ALL:
        return "all"
    elif aclOperation == AclOperation.READ:
        return "read"
    elif aclOperation == AclOperation.WRITE:
        return "write"
    elif aclOperation == AclOperation.CREATE:
        return "create"
    elif aclOperation == AclOperation.DELETE:
        return "delete"
    elif aclOperation == AclOperation.ALTER:
        return "alter"
    elif aclOperation == AclOperation.DESCRIBE:
        return "describe"
    elif aclOperation == AclOperation.CLUSTER_ACTION:
        return "cluster_action"
    elif aclOperation == AclOperation.DESCRIBE_CONFIGS:
        return "describe_configs"
    elif aclOperation == AclOperation.ALTER_CONFIGS:
        return "alter_configs"
    elif aclOperation == AclOperation.IDEMPOTENT_WRITE:
        return "itempotent_write"


def aclPermissionType_to_str(aclPermissionType):
    if aclPermissionType == AclPermissionType.UNKNOWN:
        return "UNKNOWN"
    elif aclPermissionType == AclPermissionType.ANY:
        return "ANY"
    elif aclPermissionType == AclPermissionType.DENY:
        return "DENY"
    elif aclPermissionType == AclPermissionType.ALLOW:
        return "ALLOW"


def aclBinding_to_dict(aclBinding):
    dict = {"restype": resourceType_to_str(aclBinding.restype),
            "name": aclBinding.name,
            "resource_pattern_type": resourcePatternType_to_str(aclBinding.resource_pattern_type),
            "principal": aclBinding.principal,
            "host": aclBinding.host,
            "operation": aclOperation_to_str(aclBinding.operation),
            "permission_type": aclPermissionType_to_str(aclBinding.permission_type)}
    return dict


def str_to_aclOperation(operation_str):
    operation_str1 = operation_str.lower()
    if operation_str1 == "unknown":
        return AclOperation.UNKNOWN
    elif operation_str1 == "any":
        return AclOperation.ANY
    elif operation_str1 == "all":
        return AclOperation.ALL
    elif operation_str1 == "read":
        return AclOperation.READ
    elif operation_str1 == "write":
        return AclOperation.WRITE
    elif operation_str1 == "create":
        return AclOperation.CREATE
    elif operation_str1 == "delete":
        return AclOperation.DELETE
    elif operation_str1 == "alter":
        return AclOperation.ALTER
    elif operation_str1 == "describe":
        return AclOperation.DESCRIBE
    elif operation_str1 == "cluster_action":
        return AclOperation.CLUSTER_ACTION
    elif operation_str1 == "describe_configs":
        return AclOperation.DESCRIBE_CONFIGS
    elif operation_str1 == "alter_configs":
        return AclOperation.ALTER_CONFIGS
    elif operation_str1 == "itempotent_write":
        return AclOperation.IDEMPOTENT_WRITE


def str_to_aclPermissionType(permission_type_str):
    permission_type_str1 = permission_type_str.upper()
    if permission_type_str1 == "UNKNOWN":
        return AclPermissionType.UNKNOWN
    elif permission_type_str1 == "ANY":
        return AclPermissionType.ANY
    elif permission_type_str1 == "DENY":
        return AclPermissionType.DENY
    elif permission_type_str1 == "ALLOW":
        return AclPermissionType.ALLOW


def topicMetadata_to_topic_dict(topicMetadata):
    partitions_dict = {partition_int: partitionMetadata_to_partition_dict(partitionMetadata) for partition_int, partitionMetadata in topicMetadata.partitions.items()}
    topic_dict = {"topic": topicMetadata.topic, "partitions": partitions_dict, "error": kafkaError_to_error_dict(topicMetadata.error)}
    return topic_dict


def partitionMetadata_to_partition_dict(partitionMetadata):
    partition_dict = {"id": partitionMetadata.id, "leader": partitionMetadata.leader, "replicas": partitionMetadata.replicas, "isrs": partitionMetadata.isrs, "error": kafkaError_to_error_dict(partitionMetadata.error)}
    return partition_dict


def kafkaError_to_error_dict(kafkaError):
    error_dict = None
    if kafkaError:
        error_dict = {"code": kafkaError.code(), "fatal": kafkaError.fatal(), "name": kafkaError.name(), "retriable": kafkaError.retriable(), "str": kafkaError.str(), "txn_requires_abort": kafkaError.txn_requires_abort()}
    return error_dict
