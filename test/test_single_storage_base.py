import math
import os
import sys
import time
import unittest
import warnings
from confluent_kafka import KafkaError

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from kafi.storage import *
from kafi.helpers import *

#

OFFSET_INVALID = -1001
OFFSET_END = -1

#

class TestSingleStorageBase(unittest.TestCase):
    def setUp(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        #
        self.key_snack_str_list = ['{"key": "cookie"}', '{"key": "cake"}', '{"key": "timtam"}']
        self.key_snack_bytes_list = [bytes(key_snack_str, encoding="utf-8") for key_snack_str in self.key_snack_str_list]
        self.key_snack_dict_list = [json.loads(key_snack_str) for key_snack_str in self.key_snack_str_list]
        # https://simon-aubury.medium.com/kafka-with-avro-vs-kafka-with-protobuf-vs-kafka-with-json-schema-667494cbb2af
        self.value_snack_str_list = ['{"name": "cookie", "calories": 500.0, "colour": "brown"}', '{"name": "cake", "calories": 260.0, "colour": "white"}', '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}']
        self.value_snack_bytes_list = [bytes(value_snack_str, encoding="utf-8") for value_snack_str in self.value_snack_str_list]
        self.value_snack_dict_list = [json.loads(value_snack_str) for value_snack_str in self.value_snack_str_list]
        #
        self.value_snack_ish_dict_list = []
        for value_snack_dict in self.value_snack_dict_list:
            value_snack_dict1 = value_snack_dict.copy()
            value_snack_dict1["colour"] += "ish"
            self.value_snack_ish_dict_list.append(value_snack_dict1)
        #
        self.avro_key_schema_str = '{"type": "record", "name": "snack_key", "fields": [{"name": "key", "type": "string"}]}'
        self.avro_value_schema_str = '{"type": "record", "name": "snack_value", "fields": [{"name": "name",  "type": "string"}, {"name": "calories", "type": "float"}, {"type": "string", "name": "colour"}]}'
        self.avro_value_schema_normalized_str = '{"type":"record","name":"snack_value","fields":[{"name":"name","type":"string"},{"name":"calories","type":"float"},{"name":"colour","type":"string"}]}'
        #
        self.protobuf_key_schema_str = 'message snack_key { required string key = 1; }'
        self.protobuf_value_schema_str = 'message snack_value { required string name = 1; required float calories = 2; optional string colour = 3; }'
        #
        self.jsonschema_key_schema_str = '{"title": "snack_key", "type": "object", "required": ["key"], "additionalProperties": false, "properties": {"key": {"type": "string"}}}'
        self.jsonschema_value_schema_str = '{"additionalProperties": false, "properties": {"calories": {"type": "number"}, "colour": {"type": "string"}, "name": {"type": "string"}}, "required": ["name", "calories"], "title": "snack_value", "type": "object"}'
        self.jsonschema_value_schema_normalized_str = '{"additionalProperties":false,"properties":{"calories":{"type":"number"},"colour":{"type":"string"},"name":{"type":"string"}},"required":["name","calories"],"title":"snack_value","type":"object"}'
        self.jsonschema_value_schema_normalized_redpanda_str = '{"additionalProperties":false,"properties":{"calories":{"type":"number"},"colour":{"type":"string"},"name":{"type":"string"}},"required":["name","calories"],"title":"snack_value","type":"object"}'
        self.jsonschema_value_schema_one_less_field_str = '{"title":"snack_value","type":"object","required":["name"],"additionalProperties":false,"properties":{"name":{"type":"string"},"colour":{"type":"string"}}}'
        self.jsonschema_value_schema_one_more_field_str = '{"title":"snack_value","type":"object","required":["name","calories"],"additionalProperties":false,"properties":{"name":{"type":"string"},"calories":{"type":"number"},"colour":{"type":"string"},"country":{"type":"string", "default": ""}}}'
        self.jsonschema_value_schema_one_more_field_normalized_str = '{"additionalProperties":false,"properties":{"calories":{"type":"number"},"colour":{"type":"string"},"country":{"default":"","type":"string"},"name":{"type":"string"}},"required":["name","calories"],"title":"snack_value","type":"object"}'
        self.jsonschema_value_schema_one_more_field_normalized_redpanda_str = '{"additionalProperties":false,"properties":{"name":{"type":"string"},"calories":{"type":"number"},"colour":{"type":"string"},"country":{"type":"string","default":""}},"required":["name","calories"],"title":"snack_value","type":"object"}'
        #
        self.headers_str_bytes_tuple_list = [("header_field1", b"header_value1"), ("header_field2", b"header_value2")]
        self.headers_str_bytes_dict = {"header_field1": b"header_value1", "header_field2": b"header_value2"}
        self.headers_str_str_tuple_list = [("header_field1", "header_value1"), ("header_field2", "header_value2")]
        self.headers_str_str_dict = {"header_field1": "header_value1", "header_field2": "header_value2"}
        #
        self.nested_json_str_list = [{"state": "Florida", "shortname": "FL", "info": {"governor": "Rick Scott"}, "counties": [{"name": "Dade", "population": 12345}, {"name": "Broward", "population": 40000}, {"name": "Palm Beach", "population": 60000}]}, {"state": "Ohio", "shortname": "OH", "info": {"governor": "John Kasich"}, "counties": [{"name": "Summit", "population": 1234}, {"name": "Cuyahoga", "population": 1337}]}]
        #
        self.snack_countries_str_list = ['{"snack_name": "timtam", "country": "Australia"}', '{"snack_name": "cookie", "country": "US"}']
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
            s = self.get_cluster() # comment out if using the REST Proxy to Confluent Cloud
            # pass
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
            #
            topic_str = self.create_test_topic_name()
            s.create(topic_str)
            #
            acl_dict = {"resource_type": "topic", "name": topic_str, "pattern_type": "literal", "principal": self.principal_str, "host": "*",  "operation": "read", "permission_type": "allow"}
            acl_dict1 = s.create_acl(acl_dict)
            self.assertEqual(acl_dict, acl_dict1)
            #
            time.sleep(2)
            #
            acl_dict_list1 = s.acls(acl_dict)
            self.assertIn({"resource_type": "topic", "name": topic_str, "pattern_type": "literal", 'principal': self.principal_str, 'host': '*', 'operation': 'read', 'permission_type': 'allow'}, acl_dict_list1)
            #
            acl_dict_list2 = s.delete_acls(acl_dict)
            self.assertEqual([acl_dict], acl_dict_list2)
            #
            time.sleep(2)
            #
            acl_dict_list3 = s.acls()
            self.assertNotIn(acl_dict, acl_dict_list3)

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
            # TODO: Doesn't yet work with Redpanda:
            # cimpl.KafkaException: KafkaError{code=INVALID_CONFIG,val=40,str="Setting broker properties on named brokers is unsupported"}
            if s.__class__.__name__ == "Cluster" and s.cluster_kind() != "redpanda":
                old_broker_config_dict = s.broker_config(broker_int)[broker_int]
                if "background.threads" in old_broker_config_dict:
                    old_background_threads_str = old_broker_config_dict["background.threads"]
                else:
                    old_background_threads_str = 10
                s.broker_config(broker_int, {"background.threads": 5})
                time.sleep(0.5)
                new_background_threads_str = s.broker_config(broker_int)[broker_int]["background.threads"]
                self.assertEqual(new_background_threads_str, "5")
                s.broker_config(broker_int, {"background.threads": old_background_threads_str})

    # Groups

    def test_groups(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, type="string")
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = s.consumer(topic_str, group=group_str, type="string")
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
        time.sleep(2)
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

    def test_group_offsets_new_consumer(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        s.enable_auto_commit(False)
        s.commit_after_processing(True)
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, type="str")
        producer.produce("message 1")
        producer.produce("message 2")
        producer.close()
        #
        time.sleep(1)
        #
        group_str = self.create_test_group_name()
        x = s.cat(topic_str, group=group_str, type="str", n=1)
        #
        y = s.cat(topic_str, group=group_str, type="str", n=1)

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
        group_str_topic_str_offsets_dict_dict_dict = s.group_offsets(group_str, {topic_str: {0: 2}})
        self.assertEqual(group_str_topic_str_offsets_dict_dict_dict, {group_str: {topic_str: {0: 2}}})
        [message_dict] = consumer.consume(n=1)
        consumer.commit()
        self.assertEqual(message_dict["value"], "message 3")
        #
        consumer.close()

    def test_lags(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        s.enable_auto_commit(False)
        s.commit_after_processing(False)
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str, partitions=1)
        producer = s.producer(topic_str, type="str")
        producer.produce("message 1")
        producer.produce("message 2")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = s.consumer(topic_str, group=group_str, type="str")
        consumer.consume(n=1)
        consumer.commit(asynchronous=False)
        lags_dict1 = s.lags(group_str, topic_str)
        self.assertEqual(lags_dict1[group_str][topic_str][0], 1)
        consumer.consume(n=1)
        consumer.commit()
        lags_dict2 = s.lags(group_str, topic_str)   
        self.assertEqual(lags_dict2[group_str][topic_str][0], 0)
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
        s.config(topic_str, {"retention.ms": "4711"})
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
        producer.produce("message 1")
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
        producer.produce("message 0", flush=True)
        time.sleep(0.1)
        producer.produce("message 1", flush=True)
        producer.close()
        #
        self.assertEqual(s.l(topic_str, partitions=True)[topic_str]["partitions"][0], 2)
        #
        group_str = self.create_test_group_name()
        consumer = s.consumer(topic_str, group=group_str, value_type="str")
        message_dict_list = consumer.consume(n=2)
        consumer.close()
        message0_timestamp_int = message_dict_list[0]["timestamp"][1]
        # message0_offset_int = message_dict_list[0]["offset"]
        message1_timestamp_int = message_dict_list[1]["timestamp"][1]
        message1_offset_int = message_dict_list[1]["offset"]
        #
        topic_str_offsets_dict_dict1 = s.offsets_for_times(topic_str, {0: message1_timestamp_int})
        found_message1_offset_int1 = topic_str_offsets_dict_dict1[topic_str][0]
        self.assertEqual(message1_offset_int, found_message1_offset_int1)
        #
        topic_str_offsets_dict_dict2 = s.offsets_for_times(topic_str, {topic_str: {0: message1_timestamp_int}})
        found_message1_offset_int2 = topic_str_offsets_dict_dict2[topic_str][0]
        self.assertEqual(message1_offset_int, found_message1_offset_int2)
        #
        topic_str_offsets_dict_dict3 = s.offsets_for_times(topic_str, {topic_str: {0: message1_timestamp_int + 10 * 1000}})
        found_message1_offset_int3 = topic_str_offsets_dict_dict3[topic_str][0]
        self.assertEqual(-1, found_message1_offset_int3)
        #
        topic_str_offsets_dict_dict4 = s.offsets_for_times(topic_str, {topic_str: {0: message1_timestamp_int + 10 * 1000}}, replace_not_found="last")
        found_message1_offset_int4 = topic_str_offsets_dict_dict4[topic_str][0]
        self.assertEqual(message1_offset_int, found_message1_offset_int4)
        #
        topic_str_offsets_dict_dict5 = s.offsets_for_times(topic_str, {topic_str: {0: message0_timestamp_int - 1}}, replace_not_found="first")
        found_message1_offset_int5 = topic_str_offsets_dict_dict5[topic_str][0]
        self.assertEqual(0, found_message1_offset_int5)


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
            s.partitions(topic_str, 2)
        #
        partitions_int_2 = s.partitions(topic_str)[topic_str]
        self.assertEqual(partitions_int_2, 2)
        if s.__class__.__name__ in ["Cluster", "RestProxy"]:
            topic_str_partition_int_partition_dict_dict_dict = s.partitions(topic_str, verbose=True)[topic_str]
            self.assertEqual(list(topic_str_partition_int_partition_dict_dict_dict.keys()), [0, 1])
            if s.cluster_kind() == "redpanda":
                if s.__class__.__name__ != "RestProxy":
                    self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[0]["id"], 0)
                self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[0]["leader"], 0)
                self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[0]["replicas"], [0])
                self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[0]["isrs"], [0])
                if s.__class__.__name__ != "RestProxy":
                    self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[1]["id"], 1)
                self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[1]["leader"], 0)
                self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[1]["replicas"], [0])
                self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[1]["isrs"], [0])
            else:
                if s.__class__.__name__ != "RestProxy":
                    self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[0]["id"], 0)
                self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[0]["leader"], 1)
                self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[0]["replicas"], [1])
                self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[0]["isrs"], [1])
                if s.__class__.__name__ != "RestProxy":
                    self.assertEqual(topic_str_partition_int_partition_dict_dict_dict[1]["id"], 1)
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
        producer.produce(self.value_snack_str_list, key=self.key_snack_str_list)
        producer.close()
        self.assertEqual(s.topics(topic_str, size=True, partitions=True)[topic_str]["size"], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "str" triggers the conversion into a string, and "bytes" into bytes.
        consumer = s.consumer(topic_str, group=group_str, key_type="str", value_type="bytes")
        message_dict_list = consumer.consume(n=3)
        key_str_list = [message_dict["key"] for message_dict in message_dict_list]
        value_bytes_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_str_list, self.key_snack_str_list)
        self.assertEqual(value_bytes_list, self.value_snack_bytes_list)
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
        producer.produce(self.value_snack_dict_list, key=self.key_snack_str_list)
        producer.close()
        self.assertEqual(s.topics(topic_str, size=True, partitions=True)[topic_str]["size"], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "json" triggers the conversion into a dictionary, and "str" into a string.
        consumer = s.consumer(topic_str, group=group_str, key_type="json", value_type="json")
        message_dict_list = consumer.consume(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.key_snack_dict_list)
        self.assertEqual(value_dict_list, self.value_snack_dict_list)
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
        producer = s.producer(topic_str, key_type="avro", value_type="avro", key_schema=self.avro_key_schema_str, value_schema=self.avro_value_schema_str)
        producer.produce(self.value_snack_dict_list, key=self.key_snack_bytes_list)
        producer.close()
        self.assertEqual(s.topics(topic_str, size=True, partitions=True)[topic_str]["size"], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "avro" triggers the conversion into a dictionary.
        consumer = s.consumer(topic_str, group=group_str, key_type="avro", value_type="avro")
        message_dict_list = consumer.consume(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.key_snack_dict_list)
        self.assertEqual(value_dict_list, self.value_snack_dict_list)
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
        producer = s.producer(topic_str, key_type="jsonschema", value_type="jsonschema", key_schema=self.jsonschema_key_schema_str, value_schema=self.jsonschema_value_schema_str)
        producer.produce(self.value_snack_dict_list, key=self.key_snack_str_list)
        producer.close()
        self.assertEqual(s.topics(topic_str, size=True, partitions=True)[topic_str]["size"], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the types "json" and "jsonschema" (alias = "json_sr") trigger the conversion into a dictionary.
        consumer = s.consumer(topic_str, group=group_str, key_type="json_sr", value_type="json_sr", deser_conf={"validate": False})
        message_dict_list = consumer.consume(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.key_snack_dict_list)
        self.assertEqual(value_dict_list, self.value_snack_dict_list)
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
        producer = s.producer(topic_str, key_type="protobuf", value_type="pb", key_schema=self.protobuf_key_schema_str, value_schema=self.protobuf_value_schema_str)
        producer.produce(self.value_snack_dict_list, key=self.key_snack_str_list)
        producer.close()
        self.assertEqual(s.topics(topic_str, size=True, partitions=True)[topic_str]["size"], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "protobuf" (alias = "pb") triggers the conversion into a dictionary.
        consumer = s.consumer(topic_str, group=group_str, key_type="pb", value_type="protobuf")
        message_dict_list = consumer.consume(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.key_snack_dict_list)
        self.assertEqual(value_dict_list, self.value_snack_dict_list)
        consumer.close()

    def test_produce_error_handling(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str, config={"cleanup.policy": "compact"})
        #
        producer = s.producer(topic_str)
        producer.produce("123", key="123")
        producer.close()
        self.assertEqual(s.topics(topic_str, size=True, partitions=True)[topic_str]["size"], 1)
        #
        producer = s.producer(topic_str)
        # Redpanda does not give us a KafkaError (instead: None) if we publish None as the key for compacted topics...
        if s.__class__.__name__ == "RestProxy":
            self.assertRaises(Exception, producer.produce, "123")
        # if s.__class__.__name__ == "Cluster" and s.cluster_kind() != "redpanda": 
        # self.assertRaises(Exception, producer.close)
        # else:
        producer.close()

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
        consumer.consume(n=3)
        offsets_dict = consumer.offsets()
        #
        if s.__class__.__name__ == "RestProxy":
            self.assertEqual(offsets_dict, {})
        else:
            self.assertEqual(offsets_dict[topic_str][0], OFFSET_INVALID)
        #
        consumer.commit({topic_str: {0: 1}})
        # 
        offsets_dict1 = consumer.offsets()
        offsets_dict2 = s.group_offsets(group_str)[group_str]
        self.assertEqual(offsets_dict1, offsets_dict2)
        self.assertEqual(offsets_dict1[topic_str][0], 1)
        #
        consumer.commit({0: 2})
        #
        offsets_dict1 = consumer.offsets()
        offsets_dict2 = s.group_offsets(group_str)[group_str]
        self.assertEqual(offsets_dict1, offsets_dict2)
        self.assertEqual(offsets_dict1[topic_str][0], 2)
        #
        consumer.commit()
        #
        offsets_dict1 = consumer.offsets()
        offsets_dict2 = s.group_offsets(group_str)[group_str]
        self.assertEqual(offsets_dict1, offsets_dict2)
        self.assertEqual(offsets_dict1[topic_str][0], 3)
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
        w.produce(self.value_snack_str_list)
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
        message_dict_list = s.cat(topic_str1, group=group_str2, value_type="str", n=3)
        self.assertEqual(3, len(message_dict_list))
        value_str_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(value_str_list, self.value_snack_str_list)

    def test_offsets_cache(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        s.enable_auto_commit(False)
        s.commit_after_processing(False)
        #
        topic_str1 = self.create_test_topic_name()
        s.create(topic_str1, partitions=2)
        w = s.producer(topic_str1)
        w.produce(self.value_snack_dict_list[0], partition=0)
        w.produce(self.value_snack_dict_list[1], partition=0)
        w.produce(self.value_snack_dict_list[2], partition=1)
        w.close()
        n_int1 = s.l(topic_str1)[topic_str1]
        self.assertEqual(n_int1, 3)
        #
        group_str1 = self.create_test_group_name()
        c1 = s.consumer(topic_str1, group=group_str1, partitions={topic_str1: [0]})
        message_dict_list1 = c1.consume(1)
        self.assertEqual(0, message_dict_list1[0]["partition"])
        self.assertEqual(0, message_dict_list1[0]["offset"])
        self.assertEqual(self.value_snack_dict_list[0], message_dict_list1[0]["value"])
        message_dict_list2 = c1.consume(1)
        self.assertEqual(0, message_dict_list2[0]["partition"])
        self.assertEqual(1, message_dict_list2[0]["offset"])
        self.assertEqual(self.value_snack_dict_list[1], message_dict_list2[0]["value"])
        c1.close()
        #
        group_str1 = self.create_test_group_name()
        c2 = s.consumer(topic_str1, group=group_str1, partitions={topic_str1: [1]})
        message_dict_list3 = c2.consume(1)
        self.assertEqual(1, message_dict_list3[0]["partition"])
        self.assertEqual(0, message_dict_list3[0]["offset"])
        self.assertEqual(self.value_snack_dict_list[2], message_dict_list3[0]["value"])
        c2.close()

    def test_compaction(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        # Doesn't work with Confluent Cloud.
        # TODO: Doesn't yet work with Redpanda:
        #   File "/home/ralph/kafka/kafi/./test/test_single_storage_base.py", line 791, in test_compact
        #     self.assertEqual(n_int2, 4)
        #     AssertionError: 102 != 4 
        if not self.is_ccloud() and s.__class__.__name__ == "Cluster" and s.cluster_kind() != "redpanda":
            #
            topic_str = self.create_test_topic_name()
            s.create(topic_str, config={"cleanup.policy": "compact", "max.compaction.lag.ms": 100, "min.cleanable.dirty.ratio": 0.0000000001, "segment.ms": 100, "delete.retention.ms": 100})
            #
            pr = s.producer(topic_str)
            for i in range(0, 100):
                if i % 2 == 0:
                    pr.produce(i, key="even")
                else:
                    pr.produce(i, key="odd")
            pr.flush()
            #
            (n_int1, _, _) = s.wc(topic_str)
            self.assertEqual(n_int1, 100)
            pr.produce(100, key="even")
            time.sleep(15)
            pr.produce(101, key="odd")
            pr.close()
            #
            (n_int2, _, _) = s.wc(topic_str)
            self.assertEqual(n_int2, 4)

    def test_compact(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str1 = self.create_test_topic_name()
        s.create(topic_str1)
        #
        producer = s.producer(topic_str1, type="json")
        producer.produce(self.value_snack_dict_list, key=self.key_snack_dict_list)
        producer.produce(self.value_snack_dict_list, key=self.key_snack_dict_list)
        producer.close()
        self.assertEqual(s.topics(topic_str1, size=True, partitions=True)[topic_str1]["size"], 6)
        #
        group_str1 = self.create_test_group_name()
        message_dict_list1 = s.compact(topic_str1, group=group_str1, type="json")
        self.assertEqual(3, len(message_dict_list1))
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list1]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(key_dict_list, self.key_snack_dict_list)
        self.assertEqual(value_dict_list, self.value_snack_dict_list)
        #
        topic_str2 = self.create_test_topic_name()
        s.create(topic_str2)
        group_str2 = self.create_test_group_name()
        s.compact_to(topic_str1, s, topic_str2, group=group_str2, type="json")
        group_str3 = self.create_test_group_name()
        message_dict_list2 = s.cat(topic_str2, group=group_str3, type="json")
        self.assertEqual(3, len(message_dict_list2))
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list2]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list2]
        self.assertEqual(key_dict_list, self.key_snack_dict_list)
        self.assertEqual(value_dict_list, self.value_snack_dict_list)

    # Chunking large messages

    # def test_chunking(self):
    #     if self.__class__.__name__ == "TestSingleStorageBase":
    #         return
    #     #
    #     s = self.get_storage()
    #     #
    #     topic_str1 = self.create_test_topic_name()
    #     s.create(topic_str1)
    #     #
    #     producer = s.producer(topic_str1, type="json", chunk_size=-1)
    #     producer.produce(self.value_snack_dict_list, key=self.key_snack_dict_list)
    #     producer.close()
    #     #
    #     n_int1 = s.l(topic_str1)[topic_str1]
    #     self.assertEqual(len(self.value_snack_dict_list), n_int1)
    #     #
    #     group_str1 = self.create_test_group_name()
    #     message_dict_list1 = s.cat(topic_str1, group=group_str1)
    #     value_dict_list1 = [message_dict["value"] for message_dict in message_dict_list1]
    #     self.assertEqual(self.value_snack_dict_list, value_dict_list1)
    #     #
    #     #
    #     #
    #     chunk_size_bytes_int = 10
    #     #
    #     topic_str2 = self.create_test_topic_name()
    #     s.create(topic_str2)
    #     #
    #     producer = s.producer(topic_str2, chunk_size_bytes=chunk_size_bytes_int)
    #     producer.produce(self.value_snack_dict_list, key=self.key_snack_dict_list)
    #     producer.close()
    #     # Calculate how many chunked messages we should have written to Kafka.
    #     n_int2 = s.l(topic_str2)[topic_str2]
    #     n_chunks_int = sum(
    #         math.ceil(len(value_snack_bytes) / chunk_size_bytes_int)
    #         for value_snack_bytes in self.value_snack_bytes_list)
    #     self.assertEqual(n_chunks_int, n_int2)
    #     # Automatically join these chunked messages back to their original form.
    #     group_str2 = self.create_test_group_name()
    #     message_dict_list2 = s.cat(topic_str2, group=group_str2)
    #     self.assertEqual(len(self.value_snack_dict_list), len(message_dict_list2))
    #     value_dict_list2 = [message_dict["value"] for message_dict in message_dict_list2]
    #     self.assertEqual(self.value_snack_dict_list, value_dict_list2)

    #

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

    def test_delete_records(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        if s.__class__.__name__ == "Cluster":
            topic_str = self.create_test_topic_name()
            s.create(topic_str)
            w = s.producer(topic_str, value_type="str")
            w.produce(self.value_snack_str_list)
            w.close()
            #
            n_int = s.l(topic_str)[topic_str]
            self.assertEqual(n_int, 3)
            #
            offsets = topic_str
            s.delete_records(offsets)
            time.sleep(1)
            n_int = s.l(topic_str)[topic_str]
            self.assertEqual(n_int, 0)
            ###
            topic_str = self.create_test_topic_name()
            s.create(topic_str)
            w = s.producer(topic_str, value_type="str")
            w.produce(self.value_snack_str_list)
            w.close()
            #
            n_int = s.l(topic_str)[topic_str]
            self.assertEqual(n_int, 3)
            #
            offsets = {topic_str: {0: 2}}
            s.delete_records(offsets)
            time.sleep(1)
            n_int = s.l(topic_str)[topic_str]
            self.assertEqual(n_int, 1)

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
        producer.produce(self.value_snack_str_list, headers=self.headers_str_str_dict)
        producer.close()
        #
        group_str1 = self.create_test_group_name()
        message_dict_list1 = s.cat(topic_str, group=group_str1, type="str")
        self.assertEqual(3, len(message_dict_list1))
        value_str_list1 = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(value_str_list1, self.value_snack_str_list)
        #
        if not s.__class__.__name__ == "RestProxy":
            self.assertEqual(message_dict_list1[0]["headers"], self.headers_str_bytes_tuple_list)
            #
            group_str2 = self.create_test_group_name()
            message_dict_list2 = s.cat(topic_str, group=group_str2, type="str", offsets={0:1}, n=1)
            self.assertEqual(1, len(message_dict_list2))
            self.assertEqual(message_dict_list2[0]["value"], self.value_snack_str_list[1])
            self.assertEqual(message_dict_list2[0]["headers"], self.headers_str_bytes_tuple_list)

    def test_cat_from_to_offsets_diff(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        def get_partition_int_message_dict_list_dict(message_dict_list):
            return {0: [message_dict for message_dict in message_dict_list if message_dict["partition"] == 0], 1: [message_dict for message_dict in message_dict_list if message_dict["partition"] == 1]}
        # Produce five messages to a topic with two partitions.
        # 0: snack[0],               snack[1],               snack[2]
        #    0                       1                       2
        #    ts0                     ts2                     ts4
        # 1:           countries[0],           countries[1],
        #              0                       1
        #              ts1                     ts3
        topic_str = self.create_test_topic_name()
        s.create(topic_str, partitions=2)
        producer = s.producer(topic_str, type="str")
        producer.produce(self.value_snack_str_list[0], partition=0, flush=True)
        time.sleep(0.1)
        producer.produce(self.snack_countries_str_list[0], partition=1, flush=True)
        time.sleep(0.1)
        producer.produce(self.value_snack_str_list[1], partition=0, flush=True)
        time.sleep(0.1)
        producer.produce(self.snack_countries_str_list[1], partition=1, flush=True)
        time.sleep(0.1)
        producer.produce(self.value_snack_str_list[2], partition=0, flush=True)
        producer.close()
        # Read only the messages from the second to the fourth using the offsets.
        group_str5 = self.create_test_group_name()
        message_dict_list5 = s.cat(topic_str, group=group_str5, type="str", offsets={topic_str: {0: 1, 1: 0}}, end_offsets={topic_str: {0: 1, 1: 1}})
        self.assertEqual(3, len(message_dict_list5))
        partition_int_message_dict_list_dict5 = get_partition_int_message_dict_list_dict(message_dict_list5)
        self.assertEqual(partition_int_message_dict_list_dict5[0][0]["value"], self.value_snack_str_list[1])
        self.assertEqual(partition_int_message_dict_list_dict5[1][0]["value"], self.snack_countries_str_list[0])
        self.assertEqual(partition_int_message_dict_list_dict5[1][1]["value"], self.snack_countries_str_list[1])
        # Read only the messages from the third to the end of the topic using the offsets.
        group_str6 = self.create_test_group_name()
        message_dict_list6 = s.cat(topic_str, group=group_str6, type="str", offsets={0: 1, 1: 1})
        self.assertEqual(3, len(message_dict_list6))
        partition_int_message_dict_list_dict6 = get_partition_int_message_dict_list_dict(message_dict_list6)
        self.assertEqual(partition_int_message_dict_list_dict6[0][0]["value"], self.value_snack_str_list[1])
        self.assertEqual(partition_int_message_dict_list_dict6[0][1]["value"], self.value_snack_str_list[2])
        self.assertEqual(partition_int_message_dict_list_dict6[1][0]["value"], self.snack_countries_str_list[1])
        # Skip the rest of the test for RestProxy (requires offsets_for_times() not available in the REST Proxy API).
        if s.__class__.__name__ == "RestProxy":
            return
        #
        # Read the messages.
        group_str1 = self.create_test_group_name()
        message_dict_list1 = s.cat(topic_str, group=group_str1, type="str")
        self.assertEqual(5, len(message_dict_list1))
        # Get dictionary mapping partitions to timestamps.
        partition_int_ts_int_list_dict = {0: [], 1: []}
        for message_dict in message_dict_list1:
            partition_int = message_dict["partition"]
            ts_int = message_dict["timestamp"][1]
            #
            partition_int_ts_int_list_dict[partition_int].append(ts_int)
        #
        ts_int0 = partition_int_ts_int_list_dict[0][0]
        ts_int1 = partition_int_ts_int_list_dict[1][0]
        ts_int2 = partition_int_ts_int_list_dict[0][1]
        ts_int3 = partition_int_ts_int_list_dict[1][1]
        ts_int4 = partition_int_ts_int_list_dict[0][2]
        # print(partition_int_ts_int_list_dict)
        self.assertTrue(ts_int0 < ts_int1 < ts_int2 < ts_int3 < ts_int4)
        # Read the messages from the timestamp of the first to the timestamp of the fifth message.
        group_str2 = self.create_test_group_name()
        # s.verbose(1)
        n_int2 = s.offsets_diff(topic_str, ts=ts_int0, end_ts=ts_int4)[topic_str]
        self.assertEqual(5, n_int2)
        #
        message_dict_list2 = s.cat(topic_str, group=group_str2, type="str", ts=ts_int0, end_ts=ts_int4)
        self.assertEqual(5, len(message_dict_list2))
        partition_int_message_dict_list_dict2 = get_partition_int_message_dict_list_dict(message_dict_list2)
        self.assertEqual(partition_int_message_dict_list_dict2[0][0]["value"], self.value_snack_str_list[0])
        self.assertEqual(partition_int_message_dict_list_dict2[0][1]["value"], self.value_snack_str_list[1])
        self.assertEqual(partition_int_message_dict_list_dict2[0][2]["value"], self.value_snack_str_list[2])
        self.assertEqual(partition_int_message_dict_list_dict2[1][0]["value"], self.snack_countries_str_list[0])
        self.assertEqual(partition_int_message_dict_list_dict2[1][1]["value"], self.snack_countries_str_list[1])
        # Read the messages from the timestamp of the second to the timestamp of the fourth message (we also get the fifth from partition 0).
        group_str3 = self.create_test_group_name()
        # s.verbose(1)
        n_int3 = s.offsets_diff(topic_str, ts=ts_int1, end_ts=ts_int3)[topic_str]
        self.assertEqual(4, n_int3)
        #
        message_dict_list3 = s.cat(topic_str, group=group_str3, type="str", ts=ts_int1, end_ts=ts_int3)
        self.assertEqual(4, len(message_dict_list3))
        partition_int_message_dict_list_dict3 = get_partition_int_message_dict_list_dict(message_dict_list3)
        self.assertEqual(partition_int_message_dict_list_dict3[0][0]["value"], self.value_snack_str_list[1])
        self.assertEqual(partition_int_message_dict_list_dict3[0][1]["value"], self.value_snack_str_list[2])
        self.assertEqual(partition_int_message_dict_list_dict3[1][0]["value"], self.snack_countries_str_list[0])
        self.assertEqual(partition_int_message_dict_list_dict3[1][1]["value"], self.snack_countries_str_list[1])
        # Read the messages from the timestamp of the second to the timestamp of the third message (we also get the fourth on partition 1).
        group_str4 = self.create_test_group_name()
        # s.verbose(1)
        n_int4 = s.offsets_diff(topic_str, ts=ts_int1, end_ts=ts_int2)[topic_str]
        self.assertEqual(3, n_int4)
        #
        message_dict_list4 = s.cat(topic_str, group=group_str4, type="str", ts=ts_int1, end_ts=ts_int2)
        self.assertEqual(3, len(message_dict_list4))
        partition_int_message_dict_list_dict4 = get_partition_int_message_dict_list_dict(message_dict_list4)
        self.assertEqual(partition_int_message_dict_list_dict4[0][0]["value"], self.value_snack_str_list[1])
        self.assertEqual(partition_int_message_dict_list_dict4[1][0]["value"], self.snack_countries_str_list[0])
        self.assertEqual(partition_int_message_dict_list_dict4[1][1]["value"], self.snack_countries_str_list[1])

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
        producer.produce(self.value_snack_str_list, headers=self.headers_str_str_tuple_list)
        producer.close()
        #
        group_str1 = self.create_test_group_name()
        message_dict_list1 = s.head(topic_str, group=group_str1, type="json", n=3)
        self.assertEqual(3, len(message_dict_list1))
        value_dict_list1 = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(value_dict_list1, self.value_snack_dict_list)
        #
        if not s.__class__.__name__ == "RestProxy":
            self.assertEqual(message_dict_list1[0]["headers"], self.headers_str_bytes_tuple_list)

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
        producer.produce(self.value_snack_dict_list, headers=self.headers_str_bytes_dict)
        producer.close()
        #
        group_str1 = self.create_test_group_name()
        message_dict_list1 = s.tail(topic_str, group=group_str1, type="json", n=1)
        self.assertEqual(1, len(message_dict_list1))
        self.assertEqual(message_dict_list1[0]["value"], self.value_snack_dict_list[2])
        #
        if not s.__class__.__name__ == "RestProxy":
            self.assertEqual(message_dict_list1[0]["headers"], self.headers_str_bytes_tuple_list)

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
        producer.produce(self.value_snack_bytes_list, headers=self.headers_str_bytes_tuple_list)
        producer.produce(self.value_snack_bytes_list, headers=self.headers_str_bytes_tuple_list)
        producer.produce(self.value_snack_bytes_list, headers=self.headers_str_bytes_tuple_list)
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
            message_dict_list2 = s.cat(topic_str2, group=group_str2, type="json", n=1)
            self.assertEqual(1, len(message_dict_list2))
            self.assertEqual(message_dict_list2[0]["value"], self.value_snack_ish_dict_list[0])
            self.assertEqual(message_dict_list2[0]["headers"], self.headers_str_bytes_tuple_list)
        # test that the consumer does not consume too many messages (the former bug occurred when: n % consumer_batch_size > 0, n > consumer_batch_size * 2, source topic length > n (e.g. source topic length = 100000, n = 42700, consumer_batch_size = 1000))
        topic_str3 = self.create_test_topic_name()
        s.create(topic_str3)
        #
        def flatmap_ish(message_dict):
            message_dict["value"]["colour"] += "ish"
            return [message_dict]
        #
        group_str2 = self.create_test_group_name()
        (consume_n_int1, written_n_int1) = s.cp(topic_str1, s, topic_str3, group=group_str2, source_type="json", target_type="json", consume_batch_size=3, flatmap_function=flatmap_ish, n=7)
        self.assertEqual(7, consume_n_int1)
        self.assertEqual(7, written_n_int1)

    def test_wc(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, value_type="json")
        producer.produce(self.value_snack_dict_list)
        producer.close()
        #
        group_str1 = self.create_test_group_name()
        (num_messages_int, acc_num_words_int, acc_num_bytes_int) = s.wc(topic_str, group=group_str1, type="json")
        self.assertEqual(3, num_messages_int)
        self.assertEqual(18, acc_num_words_int)
        self.assertEqual(169, acc_num_bytes_int)
    
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
        producer.produce(self.value_snack_str_list)
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
        producer.produce(self.value_snack_str_list)
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
        producer.produce(self.value_snack_str_list)
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
        message_dict_list = s.cat(topic_str2, group=group_str2, type="json", n=2)
        self.assertEqual(2, len(message_dict_list))
        self.assertEqual(500.0, message_dict_list[0]["value"]["calories"])
        self.assertEqual(260.0, message_dict_list[1]["value"]["calories"])

    # Schema Registry

    def test_sr_one_version(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        #
        key_subject_name_str = s.create_subject_name_str(topic_str, True)
        key_schema_dict = s.create_schema_dict(self.avro_key_schema_str, "AVRO")
        key_schema_id_int = s.register_schema(key_subject_name_str, key_schema_dict, normalize=True)
        #
        key_registeredSchema_dict = s.lookup_schema(key_subject_name_str, key_schema_dict)
        self.assertEqual(key_registeredSchema_dict["schema_id"], key_schema_id_int)
        self.assertEqual(key_registeredSchema_dict["subject"], key_subject_name_str)
        self.assertEqual(json.dumps(json.loads(key_registeredSchema_dict["schema"]["schema_str"])), self.avro_key_schema_str)
        self.assertEqual(key_registeredSchema_dict["schema"]["schema_type"], "AVRO")
        #
        key_schema_dict1 = s.get_schema(key_schema_id_int)
        self.assertEqual(key_schema_dict1["schema_str"], self.avro_key_schema_str.replace(" ", ""))
        #
        #
        #
        value_subject_name_str = s.create_subject_name_str(topic_str, False)
        value_schema_dict = s.create_schema_dict(self.protobuf_value_schema_str, "PROTOBUF")
        value_schema_id_int = s.register_schema(value_subject_name_str, value_schema_dict, normalize=False)
        #
        value_registeredSchema_dict = s.lookup_schema(value_subject_name_str, value_schema_dict)
        self.assertEqual(value_registeredSchema_dict["schema_id"], value_schema_id_int)
        self.assertEqual(value_registeredSchema_dict["subject"], value_subject_name_str)
        replaced_schema_str1 = value_registeredSchema_dict["schema"]["schema_str"].replace("\n", "").replace("  ", " ").replace(";}", "; }")
        if s.__class__.__name__ == "Cluster" and s.cluster_kind() == "redpanda":
            replaced_schema_str1 = replaced_schema_str1.replace('syntax = "proto2";', "")
        self.assertEqual(replaced_schema_str1, self.protobuf_value_schema_str)
        self.assertEqual(value_registeredSchema_dict["schema"]["schema_type"], "PROTOBUF")
        #
        value_schema_dict1 = s.get_schema(value_schema_id_int)
        replaced_schema_str2 = value_schema_dict1["schema_str"].replace("\n", "").replace("  ", " ").replace(";}", "; }")
        if s.__class__.__name__ == "Cluster" and s.cluster_kind() == "redpanda":
            replaced_schema_str2 = replaced_schema_str2.replace('syntax = "proto2";', "")
        self.assertEqual(replaced_schema_str2, self.protobuf_value_schema_str)
        #
        #
        #
        subject_name_str_list = s.get_subjects(topic_str + "*")
        self.assertEqual(len(subject_name_str_list), 2)
        self.assertIn(key_subject_name_str, subject_name_str_list)
        self.assertIn(value_subject_name_str, subject_name_str_list)
        #
        subject_name_str_list = s.get_subjects(topic_str + "*", deleted=True)
        self.assertEqual(len(subject_name_str_list), 2)
        self.assertIn(key_subject_name_str, subject_name_str_list)
        self.assertIn(value_subject_name_str, subject_name_str_list)
        #
        try:
            s.delete_subject(key_subject_name_str, permanent=True)
        except Exception as e:
            self.assertEqual(str(e), f"Subject '{key_subject_name_str}' was not deleted first before being permanently deleted (HTTP status code 404, SR code 40405)")
        #
        subject_name_str_version_int_list_dict = s.delete_subject(key_subject_name_str, permanent=False)
        self.assertEqual(subject_name_str_version_int_list_dict[key_subject_name_str][0], 1)
        #
        subject_name_str_list = s.get_subjects(topic_str + "*", deleted=False)
        self.assertEqual(len(subject_name_str_list), 1)
        self.assertNotIn(key_subject_name_str, subject_name_str_list)
        self.assertIn(value_subject_name_str, subject_name_str_list)
        #
        subject_name_str_list = s.get_subjects(topic_str + "*", deleted=True)
        self.assertEqual(len(subject_name_str_list), 2)
        self.assertIn(key_subject_name_str, subject_name_str_list)
        self.assertIn(value_subject_name_str, subject_name_str_list)
        #
        #
        #
        subject_name_str_version_int_list_dict = s.delete_subject(key_subject_name_str, permanent=True)
        self.assertEqual(subject_name_str_version_int_list_dict[key_subject_name_str][0], 1)
        #
        subject_name_str_list = s.get_subjects(topic_str + "*", deleted=False)
        self.assertEqual(len(subject_name_str_list), 1)
        self.assertNotIn(key_subject_name_str, subject_name_str_list)
        self.assertIn(value_subject_name_str, subject_name_str_list)
        #
        subject_name_str_list = s.get_subjects(topic_str + "*", deleted=True)
        self.assertEqual(len(subject_name_str_list), 1)
        self.assertNotIn(key_subject_name_str, subject_name_str_list)
        self.assertIn(value_subject_name_str, subject_name_str_list)
        #
        #
        #
        subject_name_str_version_int_list_dict = s.delete_subject(value_subject_name_str, permanent=False)
        self.assertEqual(subject_name_str_version_int_list_dict[value_subject_name_str][0], 1)
        #
        subject_name_str_list = s.get_subjects(topic_str + "*", deleted=False)
        self.assertEqual(len(subject_name_str_list), 0)
        self.assertNotIn(key_subject_name_str, subject_name_str_list)
        self.assertNotIn(value_subject_name_str, subject_name_str_list)
        #
        subject_name_str_list = s.get_subjects(topic_str + "*", deleted=True)
        self.assertEqual(len(subject_name_str_list), 1)
        self.assertNotIn(key_subject_name_str, subject_name_str_list)
        self.assertIn(value_subject_name_str, subject_name_str_list)
        #
        #
        #
        subject_name_str_version_int_list_dict = s.delete_subject(value_subject_name_str, permanent=True)
        self.assertEqual(subject_name_str_version_int_list_dict[value_subject_name_str][0], 1)
        #
        subject_name_str_list = s.get_subjects(topic_str + "*", deleted=False)
        self.assertEqual(len(subject_name_str_list), 0)
        self.assertNotIn(key_subject_name_str, subject_name_str_list)
        self.assertNotIn(value_subject_name_str, subject_name_str_list)
        #
        subject_name_str_list = s.get_subjects(topic_str + "*", deleted=True)
        self.assertEqual(len(subject_name_str_list), 0)
        self.assertNotIn(key_subject_name_str, subject_name_str_list)
        self.assertNotIn(value_subject_name_str, subject_name_str_list)

    def test_sr_two_versions(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        #
        value_subject_name_str = s.create_subject_name_str(topic_str, False)
        value_schema_dict = s.create_schema_dict(self.jsonschema_value_schema_str, "JSON")
        value_schema_id_int = s.register_schema(value_subject_name_str, value_schema_dict, normalize=True)
        #
        try:
            s.lookup_schema(value_subject_name_str, value_schema_dict, normalize=False)
        except Exception as e:
            self.assertEqual(str(e), "Schema not found (HTTP status code 404, SR code 40403)")
        #
        value_registeredSchema_dict = s.lookup_schema(value_subject_name_str, value_schema_dict, normalize=True)
        self.assertEqual(value_registeredSchema_dict["schema_id"], value_schema_id_int)
        self.assertEqual(value_registeredSchema_dict["subject"], value_subject_name_str)
        self.assertEqual(json.dumps(json.loads(value_registeredSchema_dict["schema"]["schema_str"])), self.jsonschema_value_schema_str)
        self.assertEqual(value_registeredSchema_dict["schema"]["schema_type"], "JSON")
        #
        try:
            s.get_compatibility(value_subject_name_str)
        except Exception as e:
            self.assertEqual(str(e), f"Subject '{value_subject_name_str}' does not have subject-level compatibility configured (HTTP status code 404, SR code 40408)")
        #
        set_level_str = s.set_compatibility(value_subject_name_str, "BACKWARD")
        self.assertEqual(set_level_str, "BACKWARD")
        #
        level_str = s.get_compatibility(value_subject_name_str)
        self.assertEqual(level_str, "BACKWARD")
        #
        value_schema_one_less_field_dict = s.create_schema_dict(self.jsonschema_value_schema_one_less_field_str, "JSON")
        is_compatible_bool = s.test_compatibility(value_subject_name_str, value_schema_one_less_field_dict, version="latest")
        self.assertFalse(is_compatible_bool)
        #
        value_schema_one_more_field_dict = s.create_schema_dict(self.jsonschema_value_schema_one_more_field_str, "JSON")
        is_compatible_bool = s.test_compatibility(value_subject_name_str, value_schema_one_more_field_dict, version="latest")
        self.assertTrue(is_compatible_bool)
        #
        #
        #
        value_schema_id_int1 = s.register_schema(value_subject_name_str, value_schema_one_more_field_dict, normalize=True)
        self.assertGreater(value_schema_id_int1, value_schema_id_int)
        #
        value_schema_one_more_field_first_then_drop_it_again_dict = s.create_schema_dict(self.jsonschema_value_schema_str, "JSON")
        is_compatible_bool = s.test_compatibility(value_subject_name_str, value_schema_one_more_field_first_then_drop_it_again_dict, version="latest")
        self.assertFalse(is_compatible_bool)
        #
        value_registeredSchema_dict1 = s.get_latest_version(value_subject_name_str)
        self.assertEqual(value_registeredSchema_dict1["schema_id"], value_schema_id_int1)
        self.assertEqual(value_registeredSchema_dict1["subject"], value_subject_name_str)
        if s.__class__.__name__ == "Cluster" and s.cluster_kind() == "redpanda":
            self.assertEqual(value_registeredSchema_dict1["schema"]["schema_str"], self.jsonschema_value_schema_one_more_field_normalized_redpanda_str)
        else:
            self.assertEqual(value_registeredSchema_dict1["schema"]["schema_str"], self.jsonschema_value_schema_one_more_field_normalized_str)
        self.assertEqual(value_registeredSchema_dict1["schema"]["schema_type"], "JSON")
        #
        value_registeredSchema_dict1 = s.get_version(value_subject_name_str, 2)
        self.assertEqual(value_registeredSchema_dict1["schema_id"], value_schema_id_int1)
        self.assertEqual(value_registeredSchema_dict1["subject"], value_subject_name_str)
        if s.__class__.__name__ == "Cluster" and s.cluster_kind() == "redpanda":
            self.assertEqual(value_registeredSchema_dict1["schema"]["schema_str"], self.jsonschema_value_schema_one_more_field_normalized_redpanda_str)
        else:
            self.assertEqual(value_registeredSchema_dict1["schema"]["schema_str"], self.jsonschema_value_schema_one_more_field_normalized_str)
        self.assertEqual(value_registeredSchema_dict1["schema"]["schema_type"], "JSON")
        #
        value_registeredSchema_dict = s.get_version(value_subject_name_str, 1)
        self.assertEqual(value_registeredSchema_dict["schema_id"], value_schema_id_int)
        self.assertEqual(value_registeredSchema_dict["subject"], value_subject_name_str)
        self.assertEqual(json.dumps(json.loads(value_registeredSchema_dict["schema"]["schema_str"])), self.jsonschema_value_schema_str)
        self.assertEqual(value_registeredSchema_dict["schema"]["schema_type"], "JSON")
        #
        version_int_list = s.get_versions(value_subject_name_str)
        self.assertEqual(version_int_list, [1, 2])
        #
        try:
            s.delete_version(value_subject_name_str, 1, permanent=True)
        except Exception as e:
            self.assertEqual(str(e), f"Subject '{value_subject_name_str}' Version 1 was not deleted first before being permanently deleted")
            # self.assertEqual(str(e), f"Subject '{key_subject_name_str}' Version 1 was not deleted first before being permanently deleted (HTTP status code 404, SR code 40407)")
        #
        version_int = s.delete_version(value_subject_name_str, 1, permanent=False)
        self.assertEqual(version_int, 1)
        #
        version_int_list = s.get_versions(value_subject_name_str)
        self.assertEqual(version_int_list, [2])
        #
        value_schema_dict = s.get_schema(value_schema_id_int)
        if s.__class__.__name__ == "Cluster" and s.cluster_kind() == "redpanda":
            self.assertEqual(value_schema_dict["schema_str"], self.jsonschema_value_schema_normalized_redpanda_str)
        else:
            self.assertEqual(value_schema_dict["schema_str"], self.jsonschema_value_schema_normalized_str)
        self.assertEqual(value_schema_dict["schema_type"], "JSON")
        #
        version_int = s.delete_version(value_subject_name_str, 1, permanent=True)
        self.assertEqual(version_int, 1)
        #
        subject_name_str_version_int_list_dict = s.delete_subject(value_subject_name_str, permanent=False)
        self.assertEqual(subject_name_str_version_int_list_dict[value_subject_name_str], [2])
        subject_name_str_version_int_list_dict = s.delete_subject(value_subject_name_str, permanent=True)
        self.assertEqual(subject_name_str_version_int_list_dict[value_subject_name_str], [2])

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
        producer.produce(self.value_snack_str_list)
        producer.close()
        #
        group_str1 = self.create_test_group_name()
        df = s.topic_to_df(topic_str1, group=group_str1, type="json")
        self.assertTrue(len(df), 3)
        self.assertTrue(df.iloc[1]["name"], "cake")
        self.assertTrue(df.iloc[1]["calories"], 260.0)
        self.assertTrue(df.iloc[1]["colour"], "brown")
        #
        topic_str2 = self.create_test_topic_name()
        s.create(topic_str2)
        s.df_to_topic(df, topic_str2)
        #
        group_str2 = self.create_test_group_name()
        message_dict_list = s.cat(topic_str2, group=group_str2, n=3, type="json")
        self.assertEqual(3, len(message_dict_list))
        self.assertEqual(500.0, message_dict_list[0]["value"]["calories"])
        self.assertEqual(260.0, message_dict_list[1]["value"]["calories"])
        self.assertEqual(80.0, message_dict_list[2]["value"]["calories"])

    def test_explode(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str1 = self.create_test_topic_name()
        s.create(topic_str1)
        producer = s.producer(topic_str1, type="json")
        producer.produce(self.nested_json_str_list)
        producer.close()
        #
        group_str1 = self.create_test_group_name()
        df1 = s.topic_to_df(topic_str1, group=group_str1, type="json")
        self.assertTrue(len(df1), 2)
        self.assertTrue(df1.iloc[0]["state"], "Florida")
        self.assertTrue(df1.iloc[1]["state"], "Ohio")
        self.assertTrue(isinstance(df1.iloc[0]["counties"], list))
        self.assertTrue(isinstance(df1.iloc[1]["counties"], list))
        #
        group_str2 = self.create_test_group_name()
        df2 = s.topic_to_df(topic_str1, group=group_str2, type="json", explode=True)
        self.assertTrue(len(df2), 5)
        self.assertTrue(df2.iloc[0]["state"], "Florida")
        self.assertTrue(df2.iloc[1]["state"], "Florida")
        self.assertTrue(df2.iloc[2]["state"], "Florida")
        self.assertTrue(df2.iloc[3]["state"], "Ohio")
        self.assertTrue(df2.iloc[4]["state"], "Ohio")
        self.assertTrue(df2.iloc[0]["info.governor"], "Rick Scott")
        self.assertTrue(df2.iloc[1]["counties.name"], "Broward")
        self.assertTrue(df2.iloc[2]["counties.population"], "60000")
        self.assertTrue(df2.iloc[3]["shortname"], "OH")
        self.assertTrue(df2.iloc[4]["counties.population"], "1337")

    # Add-Ons

    def test_repeat(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        producer = s.producer(topic_str, key_type="json", value_type="json")
        producer.produce(self.value_snack_dict_list, key=self.key_snack_dict_list, headers=self.headers_str_str_dict)
        producer.close()
        #
        n_int = s.watermarks(topic_str)[topic_str][0][1]
        self.assertTrue(n_int, 3)
        #
        s.repeat(topic_str)
        #
        n_int = s.watermarks(topic_str)[topic_str][0][1]
        self.assertTrue(n_int, 4)
        #
        message_dict_list = s.cat(topic_str, type="bytes", n=4)
        self.assertEqual(message_dict_list[2]["key"], message_dict_list[3]["key"])
        self.assertEqual(message_dict_list[2]["value"], message_dict_list[3]["value"])
        if not s.__class__.__name__ == "RestProxy":
            self.assertEqual(message_dict_list[2]["headers"], message_dict_list[3]["headers"])
        #
        s.repeat(topic_str, 2, keep_headers=False)
        #
        n_int = s.watermarks(topic_str)[topic_str][0][1]
        self.assertTrue(n_int, 6)
        #
        message_dict_list = s.cat(topic_str, type="bytes", n=6)
        self.assertEqual(message_dict_list[2]["key"], message_dict_list[4]["key"])
        self.assertEqual(message_dict_list[3]["key"], message_dict_list[5]["key"])
        self.assertEqual(message_dict_list[2]["value"], message_dict_list[4]["value"])
        self.assertEqual(message_dict_list[3]["value"], message_dict_list[5]["value"])
        if not s.__class__.__name__ == "RestProxy":
            self.assertNotEqual(message_dict_list[2]["headers"], message_dict_list[4]["headers"])
            self.assertNotEqual(message_dict_list[3]["headers"], message_dict_list[5]["headers"])

    def test_recreate(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        topic_str = self.create_test_topic_name()
        s.recreate(topic_str, partitions=2, config={"retention.ms": 4711})
        producer = s.producer(topic_str, value_type="json")
        producer.produce(self.value_snack_dict_list)
        producer.close()
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

    def test_cp_group_offsets(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        s.enable_auto_commit(False)
        s.commit_after_processing(True)
        #
        # Create topic with 3 partitions.
        topic_str = self.create_test_topic_name()
        s.create(topic_str, partitions=3)
        # Produce six messages to the individual partitions.
        producer = s.producer(topic_str)
        producer.produce(self.value_snack_str_list[0], partition=0)
        producer.produce(self.value_snack_str_list[1], partition=1)
        producer.produce(self.value_snack_str_list[2], partition=2)
        producer.produce(self.value_snack_str_list[0], partition=0)
        producer.produce(self.value_snack_str_list[1], partition=1)
        producer.produce(self.value_snack_str_list[2], partition=2)
        producer.close()
        # Create two consumer group names.
        group_str1 = self.create_test_group_name()
        time.sleep(0.1)
        group_str2 = self.create_test_group_name()
        # Read topic with the first consumer group.
        # Due to commit_after_processing=True, the sum of the offsets of the consumer group is 3.
        s.cat(topic_str, group=group_str1, type="json", n=1)
        group_str_topic_str_offsets_dict_dict_dict1 = s.group_offsets(group_str1, {topic_str: {0: 2, 1: 2, 2: 2}})
        partition_0_offset_int1 = group_str_topic_str_offsets_dict_dict_dict1[group_str1][topic_str][0]
        partition_1_offset_int1 = group_str_topic_str_offsets_dict_dict_dict1[group_str1][topic_str][1]
        partition_2_offset_int1 = group_str_topic_str_offsets_dict_dict_dict1[group_str1][topic_str][2]
        self.assertEqual(partition_0_offset_int1, 2)
        self.assertEqual(partition_1_offset_int1, 2)
        self.assertEqual(partition_2_offset_int1, 2)
        # Produce three more messages.
        producer = s.producer(topic_str)
        producer.produce(self.snack_countries_str_list[0], partition=0)
        producer.produce(self.snack_countries_str_list[0], partition=1)
        producer.produce(self.snack_countries_str_list[0], partition=2)
        producer.close()
        # Copy consumer group offsets from the first to the second consumer group.
        s.cp_group_offsets(topic_str, group_str1, s, group_str2)
        group_str_topic_str_offsets_dict_dict_dict2 = s.group_offsets(group_str2)
        partition_0_offset_int2 = group_str_topic_str_offsets_dict_dict_dict2[group_str2][topic_str][0]
        partition_1_offset_int2 = group_str_topic_str_offsets_dict_dict_dict2[group_str2][topic_str][1]
        partition_2_offset_int2 = group_str_topic_str_offsets_dict_dict_dict2[group_str2][topic_str][2]
        self.assertEqual(partition_0_offset_int2, 2)
        self.assertEqual(partition_1_offset_int2, 2)
        self.assertEqual(partition_2_offset_int2, 2)
        # Read the topic with consumer group 2.
        message_dict_list = s.cat(topic_str, group=group_str2, type="json", n=3)
        self.assertEqual(len(message_dict_list), 3)
        self.assertTrue(message_dict_list[0]["value"]["country"] == "Australia")
        self.assertTrue(message_dict_list[1]["value"]["country"] == "Australia")
        self.assertTrue(message_dict_list[2]["value"]["country"] == "Australia")

    def test_chunking(self):
        if self.__class__.__name__ == "TestSingleStorageBase":
            return
        #
        s = self.get_storage()
        #
        s.enable_auto_commit(False)
        s.commit_after_processing(True)
        #
        topic_str = self.create_test_topic_name()
        s.create(topic_str)
        #
        chunk_size_bytes_int = 10
        producer = s.producer(topic_str, chunk_size_bytes=chunk_size_bytes_int)
        producer.produce(self.snack_countries_str_list)
        producer.close()
        #
        number_of_chunks_int = sum([math.ceil(len(bytes(snack_countries_str, "UTF-8")) / chunk_size_bytes_int) for snack_countries_str in self.snack_countries_str_list])
        message_dict_list1 = s.cat(topic_str, type="bytes", dechunking=False)
        self.assertEqual(len(message_dict_list1), number_of_chunks_int)
        #
        message_dict_list2 = s.cat(topic_str, dechunking=True)
        self.assertEqual(len(message_dict_list2), len(self.snack_countries_str_list))
        for message_dict, snack_countries_str in zip(message_dict_list2, self.snack_countries_str_list):
            self.assertEqual(message_dict["value"], json.loads(snack_countries_str))
