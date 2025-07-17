from fnmatch import fnmatch
import time

from kafi.helpers import pattern_match
from kafi.kafka.kafka_admin import KafkaAdmin

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AclBinding, AclBindingFilter, AclOperation, AclPermissionType, AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource, _ConsumerGroupState, _ConsumerGroupTopicPartitions, NewPartitions, NewTopic, ResourcePatternType, ResourceType

#

OFFSET_END = -1

#

class ClusterAdmin(KafkaAdmin):
    def __init__(self, cluster_obj, **kwargs):
        super().__init__(cluster_obj, **kwargs)
        #
        self.adminClient = AdminClient(cluster_obj.kafka_config_dict)

    # ACLs

    def acls(self, acl_dict={}):
        aclBindingFilter = acl_dict_to_aclBindingFilter(acl_dict)
        #
        aclBinding_list = self.adminClient.describe_acls(aclBindingFilter).result()
        #
        return [aclBinding_to_acl_dict(aclBinding) for aclBinding in aclBinding_list]

    def create_acl(self, acl_dict):
        aclBinding = acl_dict_to_aclBinding(acl_dict)
        #
        self.adminClient.create_acls([aclBinding])[aclBinding].result()
        #
        return aclBinding_to_acl_dict(aclBinding)

    def delete_acls(self, acl_dict):
        aclBindingFilter = acl_dict_to_aclBindingFilter(acl_dict)
        #
        aclBinding_list = self.adminClient.delete_acls([aclBindingFilter])[aclBindingFilter].result()
        #
        return [aclBinding_to_acl_dict(aclBinding) for aclBinding in aclBinding_list]

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
        configResource = ConfigResource(resourceType, resource_str)
        for key_str, value_str in new_config_dict.items():
            configEntry = ConfigEntry(key_str, str(value_str), incremental_operation=AlterConfigOpType.SET)
            configResource.add_incremental_config(configEntry)
        #
        future = self.adminClient.incremental_alter_configs([configResource], validate_only=test_bool)[configResource]
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
        consumerGroupState_pattern_str_list = [state_pattern] if isinstance(state_pattern, str) else state_pattern
        #
        listConsumerGroupsResult = self.adminClient.list_consumer_groups().result()
        consumerGroupListing_list = listConsumerGroupsResult.valid
        #
        group_str_state_str_dict = {consumerGroupListing.group_id: consumerGroupState_to_str(consumerGroupListing.state) for consumerGroupListing in consumerGroupListing_list if any(fnmatch(consumerGroupListing.group_id, pattern_str) for pattern_str in pattern_str_or_str_list) and any(fnmatch(consumerGroupState_to_str(consumerGroupListing.state), consumerGroupState_pattern_str) for consumerGroupState_pattern_str in consumerGroupState_pattern_str_list)}
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

#
# AclBinding
#

def acl_dict_to_aclBinding(acl_dict):
    resourceType = resource_type_str_to_resourceType(acl_dict["resource_type"])
    #
    name_str = acl_dict["name"]
    #
    resourcePatternType = pattern_type_str_to_resourcePatternType(acl_dict["pattern_type"])
    #
    principal_str = acl_dict["principal"]
    #
    host_str = acl_dict["host"]
    #
    aclOperation = operation_str_to_aclOperation(acl_dict["operation"])
    #
    aclPermissionType = permission_type_str_to_aclPermissionType(acl_dict["permission_type"])
    #
    aclBinding = AclBinding(resourceType, name_str, resourcePatternType, principal_str, host_str, aclOperation, aclPermissionType)
    #
    return aclBinding


def aclBinding_to_acl_dict(aclBinding):
    acl_dict = {"resource_type": resourceType_to_resource_type_str(aclBinding.restype),
            "name": aclBinding.name,
            "pattern_type": resourcePatternType_to_pattern_type_str(aclBinding.resource_pattern_type),
            "principal": aclBinding.principal,
            "host": aclBinding.host,
            "operation": aclOperation_to_operation_str(aclBinding.operation),
            "permission_type": aclPermissionType_to_permission_type_str(aclBinding.permission_type)}
    return acl_dict


def resourceType_to_resource_type_str(resourceType):
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
    elif resourceType == ResourceType.TRANSACTIONAL_ID:
        return "transactional_id"
    else:
        raise Exception(f"Unsupported ResourceType: {resourceType}")


def resourcePatternType_to_pattern_type_str(resourcePatternType):
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
    else:
        raise Exception(f"Unsupported ResourcePatternType: {resourcePatternType}")


def aclOperation_to_operation_str(aclOperation):
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
    else:
        raise Exception(f"Unsupported AclOperation: {aclOperation}")


def aclPermissionType_to_permission_type_str(aclPermissionType):
    if aclPermissionType == AclPermissionType.UNKNOWN:
        return "unknown"
    elif aclPermissionType == AclPermissionType.ANY:
        return "any"
    elif aclPermissionType == AclPermissionType.DENY:
        return "deny"
    elif aclPermissionType == AclPermissionType.ALLOW:
        return "allow"
    else:
        raise Exception(f"Unsupported AclPermissionType: {aclPermissionType}")

#
# AclBindingFilter
#

def acl_dict_to_aclBindingFilter(acl_dict):
    resourceType = resource_type_str_to_resourceType(acl_dict["resource_type"]) if "resource_type" in acl_dict else ResourceType.ANY
    #
    name_str = acl_dict["name"] if "name" in acl_dict else None
    #
    resourcePatternType = pattern_type_str_to_resourcePatternType(acl_dict["pattern_type"]) if "pattern_type" in acl_dict else ResourcePatternType.ANY
    #
    principal_str = acl_dict["principal"] if "principal" in acl_dict else None
    #
    host_str = acl_dict["host"] if "host" in acl_dict else None
    #
    aclOperation = operation_str_to_aclOperation(acl_dict["operation"]) if "operation" in acl_dict else AclOperation.ANY
    #
    aclPermissionType = permission_type_str_to_aclPermissionType(acl_dict["permission_type"]) if "permission_type" in acl_dict else AclPermissionType.ANY
    #
    aclBindingFilter = AclBindingFilter(resourceType, name_str, resourcePatternType, principal_str, host_str, aclOperation, aclPermissionType)
    #
    return aclBindingFilter


def resource_type_str_to_resourceType(resource_type_str):
    resource_type_str1 = resource_type_str.lower()
    if resource_type_str1 == "unknown":
        return ResourceType.UNKNOWN
    elif resource_type_str1 == "any":
        return ResourceType.ANY
    elif resource_type_str1 == "topic":
        return ResourceType.TOPIC
    elif resource_type_str1 == "group":
        return ResourceType.GROUP
    elif resource_type_str1 == "broker":
        return ResourceType.BROKER
    elif resource_type_str1 == "transactional_id":
        return ResourceType.TRANSACTIONAL_ID
    else:
        raise Exception("Unsupported resource_type. Only \"unknown\", \"any\", \"topic\", \"group\", \"broker\" and \"transactional_id\" supported.")


def pattern_type_str_to_resourcePatternType(pattern_type_str):
    pattern_type_str1 = pattern_type_str.lower()
    if pattern_type_str1 == "unknown":
        return ResourcePatternType.UNKNOWN
    elif pattern_type_str1 == "any":
        return ResourcePatternType.ANY
    elif pattern_type_str1 == "match":
        return ResourcePatternType.MATCH
    elif pattern_type_str1 == "literal":
        return ResourcePatternType.LITERAL
    elif pattern_type_str1 == "prefixed":
        return ResourcePatternType.PREFIXED
    else:
        raise Exception("Unsupported pattern_type. Only \"unknown\", \"any\", \"match\", \"literal\" and \"prefixed\" supported.")


def operation_str_to_aclOperation(operation_str):
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
    else:
        raise Exception("Unsupported operation. Only \"unknown\", \"any\", \"all\", \"read\", \"write\", \"create\", \"delete\", \"alter\", \"describe\", \"cluster_action\", \"describe_configs\", \"alter_configs\" and \"idempotent_write\" supported.")


def permission_type_str_to_aclPermissionType(permission_type_str):
    permission_type_str1 = permission_type_str.upper()
    if permission_type_str1 == "UNKNOWN":
        return AclPermissionType.UNKNOWN
    elif permission_type_str1 == "ANY":
        return AclPermissionType.ANY
    elif permission_type_str1 == "DENY":
        return AclPermissionType.DENY
    elif permission_type_str1 == "ALLOW":
        return AclPermissionType.ALLOW
    else:
        raise Exception("Unsupported permission_type. Only \"unknown\", \"any\", \"deny\" and \"allow\" supported")
