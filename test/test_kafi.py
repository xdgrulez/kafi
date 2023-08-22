import os
import random
import sys
import tempfile
import unittest
import warnings

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from kafi.kafi import *
from kafi.helpers import *

#

config_str = "local"

class Test(unittest.TestCase):
    def setUp(self):
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
        self.headers_str_bytes_tuple_list = [("header_field1", b"header_value1"), ("header_field2", b"header_value2")]
        self.headers_str_bytes_dict = {"header_field1": b"header_value1", "header_field2": b"header_value2"}
        self.headers_str_str_tuple_list = [("header_field1", "header_value1"), ("header_field2", "header_value2")]
        self.headers_str_str_dict = {"header_field1": "header_value1", "header_field2": "header_value2"}
        #
        self.avro_key_schema_str = '{ "type": "record", "name": "mykeyrecord", "fields": [{"name": "key",  "type": "string" }] }'
        #
        self.avro_value_schema_str = '{ "type": "record", "name": "myvaluerecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        self.protobuf_key_schema_str = 'message SnackKey { required string key = 1; }'
        self.protobuf_value_schema_str = 'message SnackValue { required string name = 1; required float calories = 2; optional string colour = 3; }'
        self.jsonschema_key_schema_str = '{ "title": "abckey", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "key" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecordkey" }'
        self.jsonschema_value_schema_str = '{ "title": "abcvalue", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecordvalue" }'
        #
        self.key_schema_str_list = [self.avro_key_schema_str, self.protobuf_key_schema_str, self.jsonschema_key_schema_str]
        self.value_schema_str_list = [self.avro_value_schema_str, self.protobuf_value_schema_str, self.jsonschema_value_schema_str]
        #key    
        self.storage_str_topic_str_list_dict = {"Cluster": [], "RestProxy": [], "AzureBlob": [], "Local": [], "S3": []}
        self.storage_str_group_str_list_dict = {"Cluster": [], "RestProxy": []}
        #
        self.azureblob_s3_path_str = "test"
        self.local_path_str = f"{tempfile.gettempdir()}/kafi/test/local"
        os.makedirs(self.local_path_str, exist_ok=True)
        #
        print("Test:", self._testMethodName)

    def tearDown(self):
        c = Cluster("local")
        for group_str in self.storage_str_group_str_list_dict["Cluster"]:
            c.delete_groups(group_str)
        for group_str in self.storage_str_group_str_list_dict["RestProxy"]:
            c.delete_groups(group_str)
        for topic_str in self.storage_str_topic_str_list_dict["Cluster"]:
            c.delete(topic_str)
        #
        r = RestProxy("local")
        for topic_str in self.storage_str_topic_str_list_dict["RestProxy"]:
            c.delete(topic_str)
        #
        a = self.get_azureblob()
        for topic_str in self.storage_str_topic_str_list_dict["AzureBlob"]:
            a.delete(topic_str)
        #
        l = self.get_local()
        for topic_str in self.storage_str_topic_str_list_dict["Local"]:
            l.delete(topic_str)
        #
        s = self.get_s3()
        for topic_str in self.storage_str_topic_str_list_dict["S3"]:
            s.delete(topic_str)

    def create_test_topic_name(self, storage_obj):
        while True:
            topic_str = f"test_topic_{get_millis()}"
            #
            storage_str = storage_obj.__class__.__name__
            if topic_str not in self.storage_str_topic_str_list_dict[storage_str]:
                self.storage_str_topic_str_list_dict[storage_str].append(topic_str)
                break
        #
        return topic_str

    def create_test_group_name(self, storage_obj):
        while True:
            group_str = f"test_group_{get_millis()}"
            #
            storage_str = storage_obj.__class__.__name__
            if group_str not in self.storage_str_group_str_list_dict[storage_str]:
                self.storage_str_group_str_list_dict[storage_str].append(group_str)
                break
        #
        return group_str

    def get_azureblob(self):
        a = AzureBlob(config_str)
        a.root_dir(self.azureblob_s3_path_str)
        return a

    def get_local(self):
        l = Local(config_str)
        l.root_dir(self.local_path_str)
        return l

    def get_s3(self):
        s = S3(config_str)
        s.root_dir(self.azureblob_s3_path_str)
        return s

    # Cp from fs storage to fs storage

    def test_cp_azureblob_local(self):
        a = self.get_azureblob()
        l = self.get_local()
        #
        test_cp_fs_to_fs(self, a, l)

    def test_cp_azureblob_s3(self):
        a = self.get_azureblob()
        s = self.get_s3()
        #
        test_cp_fs_to_fs(self, a, s)

    def test_cp_local_azureblob(self):
        l = self.get_local()
        a = self.get_azureblob()
        #
        test_cp_fs_to_fs(self, l, a)

    def test_cp_local_s3(self):
        l = self.get_local()
        s = self.get_s3()
        #
        test_cp_fs_to_fs(self, l, s)

    def test_cp_s3_azureblob(self):
        s = self.get_s3()
        a = self.get_azureblob()
        #
        test_cp_fs_to_fs(self, s, a)

    def test_cp_s3_local(self):
        s = self.get_s3()
        l = self.get_local()
        #
        test_cp_fs_to_fs(self, s, l)

    # Cp from kafka storage to fs storage

    def test_cp_cluster_azureblob(self):
        c = Cluster("local")
        a = self.get_azureblob()
        #
        test_cp_kafka_to_fs(self, c, a)

    def test_cp_cluster_local(self):
        c = Cluster("local")
        l = self.get_local()
        #
        test_cp_kafka_to_fs(self, c, l)

    def test_cp_cluster_s3(self):
        c = Cluster("local")
        s = self.get_s3()
        #
        test_cp_kafka_to_fs(self, c, s)

    def test_cp_restproxy_azureblob(self):
        r = RestProxy("local")
        a = self.get_azureblob()
        #
        test_cp_kafka_to_fs(self, r, a)

    def test_cp_restproxy_local(self):
        r = RestProxy("local")
        l = self.get_local()
        #
        test_cp_kafka_to_fs(self, r, l)

    def test_cp_restproxy_s3(self):
        r = RestProxy("local")
        s = self.get_s3()
        #
        test_cp_kafka_to_fs(self, r, s)

    # Cp from fs storage to kafka storage

    def test_cp_azureblob_to_cluster(self):
        a = self.get_azureblob()
        c = Cluster("local")
        #
        test_cp_fs_to_kafka(self, a, c)

    def test_cp_local_to_cluster(self):
        l = self.get_local()
        c = Cluster("local")
        #
        test_cp_fs_to_kafka(self, l, c)

    def test_cp_s3_to_cluster(self):
        s = self.get_s3()
        c = Cluster("local")
        #
        test_cp_fs_to_kafka(self, s, c)

    def test_cp_azureblob_to_restproxy(self):
        a = self.get_azureblob()
        r = RestProxy("local")
        #
        test_cp_fs_to_kafka(self, a, r)

    def test_cp_local_to_restproxy(self):
        l = self.get_local()
        r = RestProxy("local")
        #
        test_cp_fs_to_kafka(self, l, r)

    def test_cp_s3_to_restproxy(self):
        s = self.get_s3()
        r = RestProxy("local")
        #
        test_cp_fs_to_kafka(self, s, r)

    # TODO: Cp from kafka storage to kafka storage

    def test_cp_cluster_to_restproxy(self):
        pass

    def test_cp_restproxy_to_cluster(self):
        pass

    # TODO
    # def test_diff(self):
    #     a = self.get_azureblob()
    #     #
    #     topic_str1 = self.create_test_topic_name()
    #     a.create(topic_str1)
    #     w1 = a.openw(topic_str1, value_type="str")
    #     w1.write(self.snack_str_list)
    #     w1.close()
    #     #
    #     topic_str2 = self.create_test_topic_name()
    #     w2 = a.openw(topic_str2, value_type="str")
    #     w2.write(self.snack_ish_dict_list)
    #     w2.close()
    #     #
    #     (message_dict_message_dict_tuple_list, message_counter_int1, message_counter_int2) = a.diff(topic_str1, a, topic_str2, value_type1="json", value_type2="json", n=3)
    #     self.assertEqual(3, len(message_dict_message_dict_tuple_list))
    #     self.assertEqual(3, message_counter_int1)
    #     self.assertEqual(3, message_counter_int2)

    #

def test_cp_fs_to_fs(test_obj, fs1, fs2):
    partitions_int = 3
    topic_str1 = test_obj.create_test_topic_name(fs1)
    fs1.create(topic_str1, partitions=partitions_int)
    w = fs1.openw(topic_str1, key_type="str", value_type="bytes")
    snack_dict_list = []
    for snack_dict in test_obj.snack_dict_list:
        snack_dict1 = snack_dict.copy()
        snack_dict1["name"] += "1"
        snack_dict2 = snack_dict.copy()
        snack_dict2["name"] += "2"
        snack_dict3 = snack_dict.copy()
        snack_dict3["name"] += "3"
        snack_dict_list += [snack_dict1, snack_dict2, snack_dict3]
    w.write(snack_dict_list, key=[{"key": "1"}, {"key": "1"}, {"key": "1"}, {"key": "2"}, {"key": "2"}, {"key": "2"}, {"key": "3"}, {"key": "3"}, {"key": "3"}], headers=test_obj.headers_str_bytes_tuple_list)
    w.close()
    #
    topic_str2 = test_obj.create_test_topic_name(fs2)
    fs2.create(topic_str2, partitions=partitions_int)
    #
    def map_ish(message_dict):
        message_dict["value"]["colour"] += "ish"
        return message_dict
    #
    (read_n_int, written_n_int) = fs1.cp(topic_str1, fs2, topic_str2, source_key_type="bytes", source_value_type="json", target_key_type="json", target_value_type="json", write_batch_size=2, map_function=map_ish, n=3*3)
    test_obj.assertEqual(3*3, read_n_int)
    test_obj.assertEqual(3*3, written_n_int)
    #
    (message_dict_list, n_int) = fs2.cat(topic_str2, value_type="json", n=3*3)
    test_obj.assertEqual(3*3, len(message_dict_list))
    test_obj.assertEqual(3*3, n_int)
    headers_list = [message_dict["headers"] for message_dict in message_dict_list]
    test_obj.assertEqual(headers_list[0], test_obj.headers_str_bytes_tuple_list)
    #
    # Has the mapping been done?
    for message_dict in message_dict_list:
        test_obj.assertTrue(message_dict["value"]["colour"].endswith("ish"))
    #
    # Has the order of the snacks been kept intact?
    for i in range(3):
        j0 = next(j for j, message_dict in enumerate(message_dict_list) if message_dict["value"]["name"] == snack_dict_list[3*i]["name"])
        j1 = next(j for j, message_dict in enumerate(message_dict_list) if message_dict["value"]["name"] == snack_dict_list[3*i+1]["name"])
        j2 = next(j for j, message_dict in enumerate(message_dict_list) if message_dict["value"]["name"] == snack_dict_list[3*i+2]["name"])
        #
        test_obj.assertEqual(message_dict_list[j0]["partition"], message_dict_list[j1]["partition"])
        test_obj.assertEqual(message_dict_list[j1]["partition"], message_dict_list[j2]["partition"])
        #
        test_obj.assertLess(message_dict_list[j0]["offset"], message_dict_list[j1]["offset"])
        test_obj.assertLess(message_dict_list[j1]["offset"], message_dict_list[j2]["offset"])
    #
    topic_str3 = test_obj.create_test_topic_name(fs2)
    fs2.create(topic_str3, partitions=partitions_int)
    #
    (read_n_int2, written_n_int2) = fs1.cp(topic_str1, fs2, topic_str3, source_key_type="json", source_value_type="str", target_key_type="json", target_value_type="json", write_batch_size=3, n=3*3, keep_partitions=True, keep_timestamps=True)
    test_obj.assertEqual(3*3, read_n_int2)
    test_obj.assertEqual(3*3, written_n_int2)
    #
    fs1_watermarks_dict = fs1.watermarks(topic_str1)
    fs2_watermarks_dict = fs2.watermarks(topic_str3)
    test_obj.assertEqual(list(fs1_watermarks_dict.values()), list(fs2_watermarks_dict.values()))
    #
    (message_dict_list2, n_int) = fs1.cat(topic_str1, value_type="json", n=3*3)
    (message_dict_list3, n_int) = fs2.cat(topic_str3, value_type="json", n=3*3)
    fs1_timestamp_set = set([message_dict["timestamp"] for message_dict in message_dict_list2])
    fs2_timestamp_set = set([message_dict["timestamp"] for message_dict in message_dict_list3])
    test_obj.assertEqual(fs1_timestamp_set, fs2_timestamp_set)

def test_cp_storage_to_storage(test_obj, storage1, storage2):
    partitions_int = 3
    # Create topic1 on storage1
    topic_str1 = test_obj.create_test_topic_name(storage1)
    storage1.create(topic_str1, partitions=partitions_int)
    if storage1.__class__.__name__ in ["Cluster", "RestProxy"]:
        random_int = random.randint(0, 2)
        type_str = ["avro", "protobuf", "jsonschema"][random_int]
        key_schema_str = test_obj.avro_schema_str_list

        w = storage1.openw(topic_str1, key_type="avro", value_type="avro", key_schema=test_obj.avro_key_schema_str, value_schema=test_obj.avro_value_schema_str)
    
    snack_dict_list = []
    for snack_dict in test_obj.snack_dict_list:
        snack_dict1 = snack_dict.copy()
        snack_dict1["name"] += "1"
        snack_dict2 = snack_dict.copy()
        snack_dict2["name"] += "2"
        snack_dict3 = snack_dict.copy()
        snack_dict3["name"] += "3"
        snack_dict_list += [snack_dict1, snack_dict2, snack_dict3]
    w.write(snack_dict_list, key=[{"key": "1"}, {"key": "1"}, {"key": "1"}, {"key": "2"}, {"key": "2"}, {"key": "2"}, {"key": "3"}, {"key": "3"}, {"key": "3"}], headers=test_obj.headers_str_bytes_tuple_list)
    w.close()
    #
    topic_str2 = test_obj.create_test_topic_name(storage2)
    storage2.create(topic_str2, partitions=partitions_int)
    #
    def map_ish(message_dict):
        message_dict["value"]["colour"] += "ish"
        return message_dict
    #
    group_str1 = test_obj.create_test_group_name(storage1)
    (read_n_int, written_n_int) = storage1.cp(topic_str1, storage2, topic_str2, group=group_str1, source_key_type="bytes", source_value_type="bytes", target_key_type="bytes", target_value_type="bytes", write_batch_size=2, map_function=map_ish, n=3*3)
    test_obj.assertEqual(3*3, read_n_int)
    test_obj.assertEqual(3*3, written_n_int)
    #
    topic_str3 = test_obj.create_test_topic_name(storage2)
    storage2.create(topic_str2, partitions=partitions_int)
    (read_n_int, written_n_int) = storage1.fs(topic_str2, storage2, topic_str3, group=group_str2, source_key_type="avro", source_value_type="avro", target_key_type="json", target_value_type="json", write_batch_size=2, map_function=map_ish, n=3*3)
    test_obj.assertEqual(3*3, read_n_int)
    test_obj.assertEqual(3*3, written_n_int)
    #
    (message_dict_list, n_int) = storage2.cat(topic_str2, value_type="json", n=3*3)
    test_obj.assertEqual(3*3, len(message_dict_list))
    test_obj.assertEqual(3*3, n_int)
    #
    (read_n_int, written_n_int) = storage1.cp(topic_str1, storage2, topic_str2, group=group_str1, source_key_type="avro", source_value_type="avro", target_key_type="json", target_value_type="json", write_batch_size=2, map_function=map_ish, n=3*3)
    test_obj.assertEqual(3*3, read_n_int)
    test_obj.assertEqual(3*3, written_n_int)



    # Has the mapping been done?
    for message_dict in message_dict_list:
        test_obj.assertTrue(message_dict["value"]["colour"].endswith("ish"))
    #
    # Has the order of the snacks been kept intact?
    for i in range(3):
        j0 = next(j for j, message_dict in enumerate(message_dict_list) if message_dict["value"]["name"] == snack_dict_list[3*i]["name"])
        j1 = next(j for j, message_dict in enumerate(message_dict_list) if message_dict["value"]["name"] == snack_dict_list[3*i+1]["name"])
        j2 = next(j for j, message_dict in enumerate(message_dict_list) if message_dict["value"]["name"] == snack_dict_list[3*i+2]["name"])
        #
        test_obj.assertEqual(message_dict_list[j0]["partition"], message_dict_list[j1]["partition"])
        test_obj.assertEqual(message_dict_list[j1]["partition"], message_dict_list[j2]["partition"])
        #
        test_obj.assertLess(message_dict_list[j0]["offset"], message_dict_list[j1]["offset"])
        test_obj.assertLess(message_dict_list[j1]["offset"], message_dict_list[j2]["offset"])
    #
    topic_str3 = test_obj.create_test_topic_name(storage2)
    storage2.create(topic_str3, partitions=partitions_int)
    # Test keep_partitions and keep_timestamps
    group_str2 = test_obj.create_test_group_name(storage1)
    (read_n_int2, written_n_int2) = storage1.cp(topic_str1, storage2, topic_str3, group=group_str2, source_key_type="avro", source_value_type="avro", target_key_type="json", target_value_type="json", write_batch_size=3, n=3*3, keep_partitions=True, keep_timestamps=True)
    test_obj.assertEqual(3*3, read_n_int2)
    test_obj.assertEqual(3*3, written_n_int2)
    #
    kafka_watermarks_dict = storage1.watermarks(topic_str1)
    fs_watermarks_dict = storage2.watermarks(topic_str3)
    test_obj.assertEqual(list(kafka_watermarks_dict.values()), list(fs_watermarks_dict.values()))
    #
    group_str3 = test_obj.create_test_group_name(storage1)
    if storage1.__class__.__name__ == "Cluster":
        (message_dict_list2, n_int) = storage1.cat(topic_str1, group=group_str3, value_type="avro", n=3*3)
        (message_dict_list3, n_int) = storage2.cat(topic_str3, value_type="json", n=3*3)
    elif storage1.__class__.__name__ == "RestProxy":
        c = Cluster("local")
        topic_str31 = test_obj.create_test_topic_name(storage2)
        storage2.create(topic_str31, partitions=partitions_int)
        group_str21 = test_obj.create_test_group_name(storage1)
        c.cp(topic_str1, storage2, topic_str31, group=group_str21, source_key_type="avro", source_value_type="avro", target_key_type="json", target_value_type="json", write_batch_size=3, n=3*3, keep_partitions=True, keep_timestamps=True)
        (message_dict_list2, n_int) = c.cat(topic_str1, group=group_str3, value_type="avro", n=3*3)
        (message_dict_list3, n_int) = storage2.cat(topic_str31, value_type="json", n=3*3)
    #
    kafka_timestamp_set = set([message_dict["timestamp"] for message_dict in message_dict_list2])
    fs_timestamp_set = set([message_dict["timestamp"] for message_dict in message_dict_list3])
    test_obj.assertEqual(kafka_timestamp_set, fs_timestamp_set)


def test_cp_fs_to_kafka(test_obj, fs, kafka):
    partitions_int = 3
    topic_str1 = test_obj.create_test_topic_name(fs)
    fs.create(topic_str1, partitions=partitions_int)
    w = fs.openw(topic_str1, key_type="json", value_type="json")
    snack_dict_list = []
    for snack_dict in test_obj.snack_dict_list:
        snack_dict1 = snack_dict.copy()
        snack_dict1["name"] += "1"
        snack_dict2 = snack_dict.copy()
        snack_dict2["name"] += "2"
        snack_dict3 = snack_dict.copy()
        snack_dict3["name"] += "3"
        snack_dict_list += [snack_dict1, snack_dict2, snack_dict3]
    w.write(snack_dict_list, key=[{"key": "1"}, {"key": "1"}, {"key": "1"}, {"key": "2"}, {"key": "2"}, {"key": "2"}, {"key": "3"}, {"key": "3"}, {"key": "3"}], headers=test_obj.headers_str_bytes_tuple_list)
    w.close()
    #
    topic_str2 = test_obj.create_test_topic_name(kafka)
    kafka.create(topic_str2, partitions=partitions_int)
    #
    def map_ish(message_dict):
        message_dict["value"]["colour"] += "ish"
        return message_dict
    #
    (read_n_int, written_n_int) = fs.cp(topic_str1, kafka, topic_str2, source_key_type="json", source_value_type="json", target_key_type="protobuf", target_value_type="protobuf", target_key_schema=test_obj.protobuf_key_schema_str, target_value_schema=test_obj.protobuf_value_schema_str, write_batch_size=2, map_function=map_ish, n=3*3)
    test_obj.assertEqual(3*3, read_n_int)
    test_obj.assertEqual(3*3, written_n_int)
    #
    group_str1 = test_obj.create_test_group_name(kafka)
    (message_dict_list, n_int) = kafka.cat(topic_str2, group=group_str1, key_type="protobuf", value_type="protobuf", n=3*3)
    test_obj.assertEqual(3*3, len(message_dict_list))
    test_obj.assertEqual(3*3, n_int)
    #
    if kafka.__class__.__name__ == "RestProxy":
        c = Cluster("local")
        group_str11 = test_obj.create_test_group_name(kafka)
        (message_dict_list, n_int) = c.cat(topic_str2, group=group_str11, key_type="protobuf", value_type="protobuf", n=3*3)
    #
    headers_list = [message_dict["headers"] for message_dict in message_dict_list]
    test_obj.assertEqual(headers_list[0], test_obj.headers_str_bytes_tuple_list)
    #
    # Has the mapping been done?
    for message_dict in message_dict_list:
        test_obj.assertTrue(message_dict["value"]["colour"].endswith("ish"))
    #
    # Has the order of the snacks been kept intact?
    for i in range(3):
        j0 = next(j for j, message_dict in enumerate(message_dict_list) if message_dict["value"]["name"] == snack_dict_list[3*i]["name"])
        j1 = next(j for j, message_dict in enumerate(message_dict_list) if message_dict["value"]["name"] == snack_dict_list[3*i+1]["name"])
        j2 = next(j for j, message_dict in enumerate(message_dict_list) if message_dict["value"]["name"] == snack_dict_list[3*i+2]["name"])
        #
        test_obj.assertEqual(message_dict_list[j0]["partition"], message_dict_list[j1]["partition"])
        test_obj.assertEqual(message_dict_list[j1]["partition"], message_dict_list[j2]["partition"])
        #
        test_obj.assertLess(message_dict_list[j0]["offset"], message_dict_list[j1]["offset"])
        test_obj.assertLess(message_dict_list[j1]["offset"], message_dict_list[j2]["offset"])
    #
    topic_str3 = test_obj.create_test_topic_name(kafka)
    kafka.create(topic_str3, partitions=partitions_int)
    # Test keep_partitions and keep_timestamps
    (read_n_int2, written_n_int2) = fs.cp(topic_str1, kafka, topic_str3, source_key_type="json", source_value_type="json", target_key_type="protobuf", target_value_type="protobuf", target_key_schema=test_obj.protobuf_key_schema_str, target_value_schema=test_obj.protobuf_value_schema_str, write_batch_size=3, n=3*3, keep_partitions=True, keep_timestamps=True)
    test_obj.assertEqual(3*3, read_n_int2)
    test_obj.assertEqual(3*3, written_n_int2)
    #
    fs_watermarks_dict = fs.watermarks(topic_str1)
    kafka_watermarks_dict = kafka.watermarks(topic_str3)
    test_obj.assertEqual(list(fs_watermarks_dict.values()), list(kafka_watermarks_dict.values()))
    #
    (message_dict_list2, n_int) = fs.cat(topic_str1, value_type="json", n=3*3)
    group_str3 = test_obj.create_test_group_name(kafka)
    if kafka.__class__.__name__ == "Cluster":
        (message_dict_list3, n_int) = kafka.cat(topic_str3, group=group_str3, value_type="protobuf", n=3*3)
    elif kafka.__class__.__name__ == "RestProxy":
        c = Cluster("local")
        (message_dict_list3, n_int) = c.cat(topic_str3, group=group_str3, value_type="protobuf", n=3*3)
    fs_timestamp_set = set([message_dict["timestamp"] for message_dict in message_dict_list2])
    kafka_timestamp_set = set([message_dict["timestamp"] for message_dict in message_dict_list3])
    test_obj.assertEqual(fs_timestamp_set, kafka_timestamp_set)
