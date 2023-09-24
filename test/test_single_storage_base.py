import os
import sys
import time
import unittest
import warnings

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from kafi.storage import *
from kafi.helpers import *

#

OFFSET_INVALID = -1001

class TestSingleStorageBase(unittest.TestCase):
    def setUp(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        #
        # https://simon-aubury.medium.com/kafka-with-avro-vs-kafka-with-protobuf-vs-kafka-with-json-schema-667494cbb2af
        self.snack_str_list = ['{"name": "cookie", "calories": 500.0, "colour": "brown"}', '{"name": "cake", "calories": 260.0, "colour": "white"}', '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}']
        self.snack_bytes_list = [bytes(snack_str, encoding="utf-8") for snack_str in self.snack_str_list]
        self.snack_dict_list = [json.loads(snack_str) for snack_str in self.snack_str_list]
        #
        self.snack_ish_dict_list = []
        for snack_dict in self.snack_dict_list:
            snack_dict1 = snack_dict.copy()
            snack_dict1["colour"] += "ish"
            self.snack_ish_dict_list.append(snack_dict1)
        #
        self.avro_schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        self.protobuf_schema_str = 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'
        self.jsonschema_schema_str = '{ "title": "abc", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecord" }'
        #
        self.headers_str_bytes_tuple_list = [("header_field1", b"header_value1"), ("header_field2", b"header_value2")]
        self.headers_str_bytes_dict = {"header_field1": b"header_value1", "header_field2": b"header_value2"}
        self.headers_str_str_tuple_list = [("header_field1", "header_value1"), ("header_field2", "header_value2")]
        self.headers_str_str_dict = {"header_field1": "header_value1", "header_field2": "header_value2"}
        #
        self.topic_str_list = []
        self.group_str_list = []
        #
        self.counter_int = 0
        #
        print("Test:", self._testMethodName)

    def tearDown(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        if s.__class__.__name__ == "RestProxy":
            # Use the native Kafka API to delete groups and topics created for REST Proxy tests.
            s = self.get_cluster()
        #
        for group_str in self.group_str_list:
            try:
                s.delete_groups(group_str)
            except Exception:
                pass
        for topic_str in self.topic_str_list:
            s.delete(topic_str)

    def create_test_topic_name(self):
        while True:
            topic_str = f"test_topic_{get_millis()}"
            #
            if topic_str not in self.topic_str_list:
                self.topic_str_list.append(topic_str)
                break
        #
        return topic_str

    def create_test_group_name(self):
        while True:
            group_str = f"test_group_{get_millis()}"
            #
            if group_str not in self.group_str_list:
                self.group_str_list.append(group_str)
                break
        #
        return group_str

    ### ClusterAdmin
    # ACLs

    def test_acls(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        if self.principal_str:
            s = self.get_storage()
            topic_str = self.create_test_topic_name()
            s.create(topic_str)
            s.create_acl(restype="topic", name=topic_str, resource_pattern_type="literal", principal=self.principal_str, host="*", operation="read", permission_type="allow")
            acl_dict_list = s.acls()
            self.assertIn({"restype": "topic", "name": topic_str, "resource_pattern_type": "literal", 'principal': self.principal_str, 'host': '*', 'operation': 'read', 'permission_type': 'ALLOW'}, acl_dict_list)
            s.delete_acl(restype="topic", name=topic_str, resource_pattern_type="literal", principal=self.principal_str, host="*", operation="read", permission_type="allow")
            self.assertIn({"restype": "topic", "name": topic_str, "resource_pattern_type": "literal", 'principal': self.principal_str, 'host': '*', 'operation': 'read', 'permission_type': 'ALLOW'}, acl_dict_list)

    # Brokers
    
    def test_brokers(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        if not self.is_ccloud():
            broker_dict = s.brokers()
            broker_int = list(broker_dict.keys())[0]
            broker_dict1 = s.brokers(f"{broker_int}")
            self.assertEqual(broker_dict, broker_dict1)
            broker_dict2 = s.brokers([broker_int])
            self.assertEqual(broker_dict1, broker_dict2)
            old_broker_config_dict = s.broker_config(broker_int)[broker_int]
            if "background.threads" in old_broker_config_dict:
                old_background_threads_str = old_broker_config_dict["background.threads"]
            else:
                old_background_threads_str = 10
            s.set_broker_config({"background.threads": 5}, broker_int)
            time.sleep(0.5)
            new_background_threads_str = s.broker_config(broker_int)[broker_int]["background.threads"]
            self.assertEqual(new_background_threads_str, "5")
            s.set_broker_config({"background.threads": old_background_threads_str}, broker_int)

    # Groups

    def test_groups(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, type="str")
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = s.consumer(topic_str, group=group_str, type="str")
        consumer.consume(n=1)
        #
        group_str_list1 = s.groups(["test*", "test_group*"])
        self.assertIn(group_str, group_str_list1)
        group_str_list2 = s.groups("test_group*")
        self.assertIn(group_str, group_str_list2)
        group_str_list3 = s.groups("test_group*", state_pattern=["stable"])
        self.assertIn(group_str, group_str_list3)
        group_str_state_str_dict = s.groups("test_group*", state_pattern="stab*", state=True)
        self.assertIn("stable", group_str_state_str_dict[group_str])
        group_str_list4 = s.groups(state_pattern="unknown", state=False)
        self.assertEqual(group_str_list4, [])
        #
        consumer.close()

    def test_describe_groups(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, type="str")
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = s.consumer(topic_str, group=group_str, type="str")
        consumer.consume(n=1)
        #
        group_dict = s.describe_groups(group_str)[group_str]
        self.assertEqual(group_dict["group_id"], group_str)
        self.assertEqual(group_dict["is_simple_consumer_group"], False)
        self.assertEqual(group_dict["partition_assignor"], "range")
        self.assertEqual(group_dict["state"], "stable")
        #
        if s.__class__.__name__ == "Cluster":
            self.assertEqual(group_dict["members"][0]["client_id"], "rdkafka")
            self.assertIsNone(group_dict["members"][0]["assignment"]["topic_partitions"][0]["error"])
            self.assertIsNone(group_dict["members"][0]["assignment"]["topic_partitions"][0]["metadata"])
            self.assertEqual(group_dict["members"][0]["assignment"]["topic_partitions"][0]["offset"], -1001)
            self.assertEqual(group_dict["members"][0]["assignment"]["topic_partitions"][0]["partition"], 0)
            self.assertEqual(group_dict["members"][0]["assignment"]["topic_partitions"][0]["topic"], topic_str)
            self.assertIsNone(group_dict["members"][0]["group_instance_id"])
            #
            broker_int = list(s.brokers().keys())[0]
            self.assertEqual(group_dict["coordinator"]["id"], broker_int)
            self.assertEqual(group_dict["coordinator"]["id_string"], f"{broker_int}")
        elif s.__class__.__name__ == "RestProxy":
            self.assertEqual(group_dict["members"][0]["client_id"][:len(f"consumer-{group_str}")], f"consumer-{group_str}")
            self.assertIsNone(group_dict["members"][0]["group_instance_id"])
        #
        consumer.close()

    def test_delete_groups(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, value_type="str")
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = s.consumer(topic_str, group=group_str, value_type="str")
        consumer.consume(n=1)
        consumer.close()
        #
        group_str_list = s.groups(group_str, state_pattern="*")
        self.assertEqual(group_str_list, [group_str])
        group_str_list = s.delete_groups(group_str, state_pattern="*")
        self.assertEqual(group_str_list, [group_str])
        group_str_list = s.groups(group_str, state_pattern="*")
        self.assertEqual(group_str_list, [])

    def test_group_offsets(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        s.enable_auto_commit(False)
        s.commit_after_processing(False)
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str, partitions=2)
        producer = s.producer(topic_str, type="str")
        producer.produce("message 1", partition=0)
        producer.produce("message 2", partition=1)
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = s.consumer(topic_str, group=group_str, type="str")
        consumer.consume(n=1)
        consumer.commit()
        consumer.consume(n=1)
        consumer.commit()
        #
        group_str_topic_str_offsets_dict_dict_dict = s.group_offsets(group_str)
        self.assertIn(group_str, group_str_topic_str_offsets_dict_dict_dict)
        self.assertIn(topic_str, group_str_topic_str_offsets_dict_dict_dict[group_str])
        self.assertEqual(group_str_topic_str_offsets_dict_dict_dict[group_str][topic_str][0], 1)
        self.assertEqual(group_str_topic_str_offsets_dict_dict_dict[group_str][topic_str][1], 1)
        #
        if s.__class__.__name__ == "Cluster":
            member_id_str = consumer.memberid()
            self.assertEqual("rdkafka-", member_id_str[:8])
        #
        consumer.close()

    def test_set_group_offsets(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        s.enable_auto_commit(False)
        s.commit_after_processing(False)
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, type="str")
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = s.consumer(topic_str, group=group_str, type="str")
        group_str_topic_str_offsets_dict_dict_dict = s.set_group_offsets({group_str: {topic_str: {0: 2}}})
        self.assertEqual(group_str_topic_str_offsets_dict_dict_dict, {group_str: {topic_str: {0: 2}}})
        [message_dict] = consumer.consume(n=1)
        consumer.commit()
        self.assertEqual(message_dict["value"], "message 3")
        #
        consumer.close()

    # Topics

    def test_config_set_config(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.touch(topic_str)
        #
        s.set_config(topic_str, {"retention.ms": "4711"})
        new_retention_ms_str = s.config(topic_str)[topic_str]["retention.ms"]
        self.assertEqual(new_retention_ms_str, "4711")

    def test_create_delete(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.touch(topic_str)
        topic_str_list = s.ls()
        self.assertIn(topic_str, topic_str_list)
        s.rm(topic_str)
        topic_str_list = s.ls()
        self.assertNotIn(topic_str, topic_str_list)

    def test_topics(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        old_topic_str_list = s.topics(["test_*"])
        self.assertNotIn(topic_str, old_topic_str_list)
        s.create(topic_str)
        new_topic_str_list = s.ls(["test_*"])
        self.assertIn(topic_str, new_topic_str_list)
        #
        producer = s.producer(topic_str, type="str")
        producer.produce("message 1", on_delivery=lambda kafkaError, message: print(kafkaError, message))
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        topic_str_size_int_dict_l = s.l(pattern=topic_str)
        topic_str_size_int_dict_ll = s.ll(pattern=topic_str)
        self.assertEqual(topic_str_size_int_dict_l, topic_str_size_int_dict_ll)
        size_int = topic_str_size_int_dict_l[topic_str]
        self.assertEqual(size_int, 3)
        topic_str_size_int_partitions_dict_dict = s.topics(pattern=topic_str, size=True, partitions=True)
        self.assertEqual(topic_str_size_int_partitions_dict_dict[topic_str]["size"], 3)
        self.assertEqual(topic_str_size_int_partitions_dict_dict[topic_str]["partitions"][0], 3)
        topic_str_partitions_dict_dict = s.l(pattern=topic_str, size=False, partitions=True)
        self.assertEqual(topic_str_partitions_dict_dict[topic_str][0], 3)

    def test_offsets_for_times(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        s.verbose(1)
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, value_type="str")
        producer.produce("message 1")
        time.sleep(1)
        producer.produce("message 2")
        producer.close()
        #
        self.assertEqual(s.l(topic_str, partitions=True)[topic_str]["partitions"][0], 2)
        #
        group_str = self.create_test_group_name()
        consumer = s.consumer(topic_str, group=group_str, value_type="str")
        message_dict_list = consumer.consume(n=2)
        consumer.close()
        message1_timestamp_int = message_dict_list[1]["timestamp"][1]
        message1_offset_int = message_dict_list[1]["offset"]
        #
        topic_str_offsets_dict_dict = s.offsets_for_times(topic_str, {0: message1_timestamp_int})
        found_message1_offset_int = topic_str_offsets_dict_dict[topic_str][0]
        self.assertEqual(message1_offset_int, found_message1_offset_int)

    def test_partitions_set_partitions(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        #
        if s.__class__.__name__ == "RestProxy":
            s.create(topic_str, partitions=2)
        else:
            s.create(topic_str)
            partitions_int_1 = s.partitions(topic_str)[topic_str]
            self.assertEqual(partitions_int_1, 1)
            s.set_partitions(topic_str, 2)
        #
        partitions_int_2 = s.partitions(topic_str)[topic_str]
        self.assertEqual(partitions_int_2, 2)
        if s.__class__.__name__ in ["Cluster", "RestProxy"]:
            topic_str_partition_int_partition_dict_dict_dict = s.partitions(topic_str, verbose=True)[topic_str]
            self.assertEqual(list(topic_str_partition_int_partition_dict_dict_dict.keys()), [0, 1])
            self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[0]["leader"], 1)
            self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[0]["replicas"], [1])
            self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[0]["isrs"], [1])
            self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[1]["leader"], 1)
            self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[1]["replicas"], [1])
            self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[1]["isrs"], [1])

    def test_exists(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        self.assertFalse(s.exists(topic_str))
        s.create(topic_str)
        self.assertTrue(s.exists(topic_str))

    # Produce/Consume

    def test_produce_consume_bytes_str(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        # Upon produce, the types "bytes" and "string" trigger the conversion of bytes, strings and dictionaries to bytes on Kafka.
        producer = s.producer(topic_str, key_type="bytes", value_type="str")
        producer.produce(self.snack_str_list, key=self.snack_str_list)
        producer.close()
        self.assertEqual(s.topics(topic_str, size=True, partitions=True)[topic_str]["size"], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "str" triggers the conversion into a string, and "bytes" into bytes.
        consumer = s.consumer(topic_str, group=group_str, key_type="str", value_type="bytes")
        message_dict_list = consumer.consume(n=3)
        key_str_list = [message_dict["key"] for message_dict in message_dict_list]
        value_bytes_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_str_list, self.snack_str_list)
        self.assertEqual(value_bytes_list, self.snack_bytes_list)
        consumer.close()
    
    def test_produce_consume_json(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        # Upon produce, the types "str" and "json" trigger the conversion of bytes, strings and dictionaries to bytes on Kafka.
        producer = s.producer(topic_str, key_type="json", value_type="json")
        producer.produce(self.snack_dict_list, key=self.snack_str_list)
        producer.close()
        self.assertEqual(s.topics(topic_str, size=True, partitions=True)[topic_str]["size"], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "json" triggers the conversion into a dictionary, and "str" into a string.
        consumer = s.consumer(topic_str, group=group_str, key_type="json", value_type="json")
        message_dict_list = consumer.consume(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.snack_dict_list)
        self.assertEqual(value_dict_list, self.snack_dict_list)
        consumer.close()

    def test_produce_consume_protobuf(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        # Upon produce, the type "protobuf" (alias = "pb") triggers the conversion of bytes, strings and dictionaries into Protobuf-encoded bytes on Kafka.
        producer = s.producer(topic_str, key_type="protobuf", value_type="pb", key_schema=self.protobuf_schema_str, value_schema=self.protobuf_schema_str)
        producer.produce(self.snack_dict_list, key=self.snack_str_list)
        producer.close()
        self.assertEqual(s.topics(topic_str, size=True, partitions=True)[topic_str]["size"], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "protobuf" (alias = "pb") triggers the conversion into a dictionary.
        consumer = s.consumer(topic_str, group=group_str, key_type="pb", value_type="protobuf")
        message_dict_list = consumer.consume(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.snack_dict_list)
        self.assertEqual(value_dict_list, self.snack_dict_list)
        consumer.close()

    def test_produce_consume_avro(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        # Upon produce, the type "avro" triggers the conversion of bytes, strings and dictionaries into Avro-encoded bytes on Kafka.
        producer = s.producer(topic_str, key_type="avro", value_type="avro", key_schema=self.avro_schema_str, value_schema=self.avro_schema_str)
        producer.produce(self.snack_dict_list, key=self.snack_bytes_list)
        producer.close()
        self.assertEqual(s.topics(topic_str, size=True, partitions=True)[topic_str]["size"], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "avro" triggers the conversion into a dictionary.
        consumer = s.consumer(topic_str, group=group_str, key_type="avro", value_type="avro")
        message_dict_list = consumer.consume(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.snack_dict_list)
        self.assertEqual(value_dict_list, self.snack_dict_list)
        consumer.close()

    def test_produce_consume_jsonschema(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        # Upon produce, the type "jsonschema" triggers the conversion of bytes, strings and dictionaries into JSONSchema-encoded bytes on Kafka.
        producer = s.producer(topic_str, key_type="jsonschema", value_type="jsonschema", key_schema=self.jsonschema_schema_str, value_schema=self.jsonschema_schema_str)
        producer.produce(self.snack_dict_list, key=self.snack_str_list)
        producer.close()
        self.assertEqual(s.topics(topic_str, size=True, partitions=True)[topic_str]["size"], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the types "json" and "jsonschema" (alias = "json_sr") trigger the conversion into a dictionary.
        consumer = s.consumer(topic_str, group=group_str, key_type="json_sr", value_type="json_sr")
        message_dict_list = consumer.consume(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.snack_dict_list)
        self.assertEqual(value_dict_list, self.snack_dict_list)
        consumer.close()

    def test_consume_from_offsets(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, value_type="str")
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = s.consumer(topic_str, group=group_str, value_type="str", offsets={0: 2})
        message_dict_list = consumer.consume(n=1)
        self.assertEqual(len(message_dict_list), 1)
        self.assertEqual(message_dict_list[0]["value"], "message 3")
        consumer.close()

    def test_commit(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        s.enable_auto_commit(False)
        s.commit_after_processing(False)
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, type="str")
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = s.consumer(topic_str, group=group_str, type="str")
        consumer.consume(n=1)
        offsets_dict = consumer.offsets()
        #
        if s.__class__.__name__ == "RestProxy":
            self.assertEqual(offsets_dict, {})
        else:
            self.assertEqual(offsets_dict[topic_str][0], OFFSET_INVALID)
        #
        consumer.commit()
        offsets_dict1 = consumer.offsets()
        #
        if s.__class__.__name__ == "RestProxy":
            self.assertEqual(offsets_dict1[topic_str][0], 3)
        else:
            self.assertEqual(offsets_dict1[topic_str][0], 1)
        #
        consumer.close()
    
    def test_error_handling(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        s.enable_auto_commit(False)
        s.commit_after_processing(True)
        #
        topic_str1 = self.create_test_topic_name()
        s.create(topic_str1)
        w = s.producer(topic_str1, value_type="str")
        w.produce(self.snack_str_list)
        w.close()
        n_int1 = s.l(topic_str1)[topic_str1]
        self.assertEqual(n_int1, 3)
        #
        self.counter_int = 0
        def map_function(message_dict):
            if self.counter_int == 2:
                self.counter_int += 1
                raise Exception("Error...")
            #
            self.counter_int += 1
            #
            return message_dict

        topic_str2 = self.create_test_topic_name()
        s.create(topic_str2)
        group_str1 = self.create_test_group_name()
        #
        try:
            s.cp(topic_str1, s, topic_str2, group=group_str1, n=3, map_function=map_function, value_type="str", consume_batch_size=1, produce_batch_size=1)
        except Exception:
            pass
        #
        n_int2 = s.l(topic_str2)[topic_str2]
        self.assertEqual(n_int2, 2)
        offset_int = s.group_offsets(group_str1)[group_str1][topic_str1][0]
        self.assertEqual(offset_int, 2)
        # s.cp(topic_str1, s, topic_str2, group=group_str, n=1, consume_batch_size=1, produce_batch_size=1)
        # n_int2 = s.l(topic_str2)[topic_str2]
        # self.assertEqual(n_int2, 3)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list, n_int1) = s.cat(topic_str1, group=group_str2, value_type="str", n=3)
        self.assertEqual(3, len(message_dict_list))
        self.assertEqual(3, n_int1)
        value_str_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(value_str_list, self.snack_str_list)

    def test_cluster_settings(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        s.verbose(0)
        self.assertEqual(s.verbose(), 0)

    def test_configs(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        config_str_list1 = s.configs()
        self.assertIn("local", config_str_list1)
        config_str_list2 = s.configs("loc*")
        self.assertIn("local", config_str_list2)
        config_str_list3 = s.configs("this_pattern_shall_not_match_anything")
        self.assertEqual(config_str_list3, [])
        #
        config_str_config_dict_dict = s.configs(verbose=True)
        self.assertIn("local", config_str_config_dict_dict)

    # Shell

    # Shell.cat -> Functional.map -> Functional.flatmap -> Functional.foldl -> ClusterConsumer.consumer/KafkaConsumer.foldl/ClusterConsumer.close -> ClusterConsumer.consume
    def test_cat(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, type="str")
        producer.produce(self.snack_str_list, headers=self.headers_str_str_dict)
        producer.close()
        #
        group_str1 = self.create_test_group_name()
        (message_dict_list1, n_int1) = s.cat(topic_str, group=group_str1, type="str")
        self.assertEqual(3, len(message_dict_list1))
        self.assertEqual(3, n_int1)
        value_str_list1 = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(value_str_list1, self.snack_str_list)
        #
        if not s.__class__.__name__ == "RestProxy":
            self.assertEqual(message_dict_list1[0]["headers"], self.headers_str_bytes_tuple_list)
            #
            group_str2 = self.create_test_group_name()
            (message_dict_list2, n_int2) = s.cat(topic_str, group=group_str2, type="str", offsets={0:1}, n=1)
            self.assertEqual(1, len(message_dict_list2))
            self.assertEqual(1, n_int2)
            self.assertEqual(message_dict_list2[0]["value"], self.snack_str_list[1])
            self.assertEqual(message_dict_list2[0]["headers"], self.headers_str_bytes_tuple_list)

    # Shell.head -> Shell.cat
    def test_head(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, value_type="json")
        producer.produce(self.snack_str_list, headers=self.headers_str_str_tuple_list)
        producer.close()
        #
        group_str1 = self.create_test_group_name()
        (message_dict_list1, n_int1) = s.head(topic_str, group=group_str1, type="json", n=3)
        self.assertEqual(3, len(message_dict_list1))
        self.assertEqual(3, n_int1)
        value_dict_list1 = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(value_dict_list1, self.snack_dict_list)
        #
        if not s.__class__.__name__ == "RestProxy":
            self.assertEqual(message_dict_list1[0]["headers"], self.headers_str_bytes_tuple_list)
            #
            group_str2 = self.create_test_group_name()
            (message_dict_list2, n_int2) = s.head(topic_str, group=group_str2, offsets={0:1}, type="json", n=1)
            self.assertEqual(1, len(message_dict_list2))
            self.assertEqual(1, n_int2)
            self.assertEqual(message_dict_list2[0]["value"], self.snack_dict_list[1])
            self.assertEqual(message_dict_list2[0]["headers"], self.headers_str_bytes_tuple_list)

    # Shell.tail -> Functional.map -> Functional.flatmap -> Functional.foldl -> ClusterConsumer.consumer/KafkaConsumer.foldl/ClusterConsumer.close -> ClusterConsumer.consume
    def test_tail(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, value_type="json")
        producer.produce(self.snack_dict_list, headers=self.headers_str_bytes_dict)
        producer.close()
        #
        group_str1 = self.create_test_group_name()
        (message_dict_list1, n_int1) = s.tail(topic_str, group=group_str1, type="json", n=3)
        self.assertEqual(3, len(message_dict_list1))
        self.assertEqual(3, n_int1)
        value_dict_list1 = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(value_dict_list1, self.snack_dict_list)
        #
        if not s.__class__.__name__ == "RestProxy":
            self.assertEqual(message_dict_list1[0]["headers"], self.headers_str_bytes_tuple_list)
            #
            group_str2 = self.create_test_group_name()
            (message_dict_list2, n_int2) = s.tail(topic_str, group=group_str2, type="json", n=1)
            self.assertEqual(1, len(message_dict_list2))
            self.assertEqual(1, n_int2)
            self.assertEqual(message_dict_list2[0]["value"], self.snack_dict_list[2])
            self.assertEqual(message_dict_list2[0]["headers"], self.headers_str_bytes_tuple_list)

    # Shell.cp -> Functional.map_to -> Functional.flatmap_to -> ClusterConsumer.producer/Functional.foldl/ClusterConsumer.close -> ClusterConsumer.consumer/KafkaConsumer.foldl/ClusterConsumer.close -> ClusterConsumer.consume
    def test_cp(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str1 = self.create_test_topic_name()
        s.create(topic_str1)
        producer = s.producer(topic_str1, value_type="json")
        producer.produce(self.snack_bytes_list, headers=self.headers_str_bytes_tuple_list)
        producer.close()
        #
        topic_str2 = self.create_test_topic_name()
        s.create(topic_str2)
        #
        def map_ish(message_dict):
            message_dict["value"]["colour"] += "ish"
            return message_dict
        #
        group_str1 = self.create_test_group_name()
        (consume_n_int, written_n_int) = s.cp(topic_str1, s, topic_str2, group=group_str1, source_type="json", target_type="json", produce_batch_size=2, map_function=map_ish, n=3)
        self.assertEqual(3, consume_n_int)
        self.assertEqual(3, written_n_int)
        #
        if not s.__class__.__name__ == "RestProxy":
            group_str2 = self.create_test_group_name()
            (message_dict_list2, n_int2) = s.cat(topic_str2, group=group_str2, type="json", n=1)
            self.assertEqual(1, len(message_dict_list2))
            self.assertEqual(1, n_int2)
            self.assertEqual(message_dict_list2[0]["value"], self.snack_ish_dict_list[0])
            self.assertEqual(message_dict_list2[0]["headers"], self.headers_str_bytes_tuple_list)

    def test_wc(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, value_type="json")
        producer.produce(self.snack_dict_list)
        producer.close()
        #
        group_str1 = self.create_test_group_name()
        (num_messages_int, acc_num_words_int, acc_num_bytes_int) = s.wc(topic_str, group=group_str1, type="json")
        self.assertEqual(3, num_messages_int)
        self.assertEqual(18, acc_num_words_int)
        self.assertEqual(169, acc_num_bytes_int)

    # Shell.diff -> Shell.diff_fun -> Functional.zipfoldl -> ClusterConsumer.consumer/read/close
    def test_diff(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str1 = self.create_test_topic_name()
        s.create(topic_str1)
        w1 = s.producer(topic_str1, value_type="json")
        w1.produce(self.snack_str_list)
        w1.close()
        #
        topic_str2 = self.create_test_topic_name()
        s.create(topic_str2)
        w2 = s.producer(topic_str2, value_type="json")
        w2.produce(self.snack_ish_dict_list)
        w2.close()
        #
        group_str1 = self.create_test_group_name()
        group_str2 = self.create_test_group_name()
        (message_dict_message_dict_tuple_list, message_counter_int1, message_counter_int2) = s.diff(topic_str1, s, topic_str2, group1=group_str1, group2=group_str2, type1="json", type2="json", n=3)
        self.assertEqual(3, len(message_dict_message_dict_tuple_list))
        self.assertEqual(3, message_counter_int1)
        self.assertEqual(3, message_counter_int2)

    # Shell.diff -> Shell.diff_fun -> Functional.flatmap -> Functional.foldl -> ClusterConsumer.open/Kafka.foldl/ClusterConsumer.close -> ClusterConsumer.consume 
    def test_grep(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, value_type="json")
        producer.produce(self.snack_str_list)
        producer.close()
        #
        group_str = self.create_test_group_name()
        (message_dict_message_dict_tuple_list, message_counter_int1, message_counter_int2) = s.grep(topic_str, ".*brown.*", group=group_str, type="json", n=3)
        self.assertEqual(1, len(message_dict_message_dict_tuple_list))
        self.assertEqual(1, message_counter_int1)
        self.assertEqual(3, message_counter_int2)
    
    # Functional

    def test_foreach(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, value_type="json")
        producer.produce(self.snack_str_list)
        producer.close()
        #
        colour_str_list = []
        group_str = self.create_test_group_name()
        s.foreach(topic_str, group=group_str, foreach_function=lambda message_dict: colour_str_list.append(message_dict["value"]["colour"]), type="json")
        self.assertEqual("brown", colour_str_list[0])
        self.assertEqual("white", colour_str_list[1])
        self.assertEqual("chocolate", colour_str_list[2])

    def test_filter(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, value_type="json")
        producer.produce(self.snack_str_list)
        producer.close()
        #
        group_str = self.create_test_group_name()
        (message_dict_list, message_counter_int) = s.filter(topic_str, group=group_str, filter_function=lambda message_dict: message_dict["value"]["calories"] > 100, type="json")
        self.assertEqual(2, len(message_dict_list))
        self.assertEqual(3, message_counter_int)

    def test_filter_to(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str1 = self.create_test_topic_name()
        s.create(topic_str1)
        producer = s.producer(topic_str1, value_type="json")
        producer.produce(self.snack_str_list)
        producer.close()
        #
        topic_str2 = self.create_test_topic_name()
        s.create(topic_str2)
        #
        group_str1 = self.create_test_group_name()
        (consume_n_int, written_n_int) = s.filter_to(topic_str1, s, topic_str2, group=group_str1, filter_function=lambda message_dict: message_dict["value"]["calories"] > 100, source_type="json", target_type="json")
        self.assertEqual(3, consume_n_int)
        self.assertEqual(2, written_n_int)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list, n_int) = s.cat(topic_str2, group=group_str2, type="json", n=2)
        self.assertEqual(2, len(message_dict_list))
        self.assertEqual(2, n_int)
        self.assertEqual(500.0, message_dict_list[0]["value"]["calories"])
        self.assertEqual(260.0, message_dict_list[1]["value"]["calories"])

    # Pandas

    def test_from_to_df(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str1 = self.create_test_topic_name()
        s.create(topic_str1)
        producer = s.producer(topic_str1, type="json")
        producer.produce(self.snack_str_list)
        producer.close()
        #
        group_str1 = self.create_test_group_name()
        df = s.to_df(topic_str1, group=group_str1, type="json")
        self.assertTrue(len(df), 3)
        self.assertTrue(df.iloc[1]["name"], "cake")
        self.assertTrue(df.iloc[1]["calories"], 260.0)
        self.assertTrue(df.iloc[1]["colour"], "brown")
        #
        topic_str2 = self.create_test_topic_name()
        s.create(topic_str2)
        s.from_df(df, topic_str2)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list, n_int2) = s.cat(topic_str2, group=group_str2, n=3, type="json")
        self.assertEqual(3, len(message_dict_list))
        self.assertEqual(3, n_int2)
        self.assertEqual(500.0, message_dict_list[0]["value"]["calories"])
        self.assertEqual(260.0, message_dict_list[1]["value"]["calories"])
        self.assertEqual(80.0, message_dict_list[2]["value"]["calories"])

    # Add-Ons

    def test_recreate(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str, partitions=2, config={"retention.ms": 4711})
        w = s.producer(topic_str, value_type="json")
        w.produce(self.snack_dict_list)
        w.close()
        #
        n_int1 = s.l(topic_str)[topic_str]
        self.assertEqual(3, n_int1)
        #
        config_dict1 = s.config(topic_str)[topic_str]
        self.assertEqual(4711, int(config_dict1["retention.ms"]))
        #
        partitions_int1 = s.partitions(topic_str)[topic_str]
        self.assertEqual(2, partitions_int1)
        #
        s.retouch(topic_str)
        #
        n_int2 = s.l(topic_str)[topic_str]
        self.assertEqual(0, n_int2)
        #
        config_dict2 = s.config(topic_str)[topic_str]
        self.assertEqual(4711, int(config_dict2["retention.ms"]))
        #
        partitions_int2 = s.partitions(topic_str)[topic_str]
        self.assertEqual(2, partitions_int2)
