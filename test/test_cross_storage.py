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
        self.avro_value_schema_str = '{ "type": "record", "name": "myvaluerecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        self.protobuf_key_schema_str = 'message SnackKey { required string key = 1; }'
        self.protobuf_value_schema_str = 'message SnackValue { required string name = 1; required float calories = 2; optional string colour = 3; }'
        self.jsonschema_key_schema_str = '{ "title": "abckey", "definitions" : { "record:myrecordkey" : { "type" : "object", "required" : [ "key" ], "additionalProperties" : false, "properties" : { "key" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecordkey" }'
        self.jsonschema_value_schema_str = '{ "title": "abcvalue", "definitions" : { "record:myrecordvalue" : { "type" : "object", "required" : [ "name", "calories", "colour" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecordvalue" }'
        #
        self.key_schema_str_list = [self.avro_key_schema_str, self.protobuf_key_schema_str, self.jsonschema_key_schema_str]
        self.value_schema_str_list = [self.avro_value_schema_str, self.protobuf_value_schema_str, self.jsonschema_value_schema_str]
        #
        self.storage_str_topic_str_list_dict = {"Cluster": [], "RestProxy": [], "AzureBlob": [], "Local": [], "S3": []}
        self.storage_str_group_str_list_dict = {"Cluster": [], "RestProxy": [], "AzureBlob": [], "Local": [], "S3": []}
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
            r.delete(topic_str)
        #
        a = self.get_azureblob()
        for group_str in self.storage_str_group_str_list_dict["AzureBlob"]:
#            a.delete_groups(group_str)
            pass
        for topic_str in self.storage_str_topic_str_list_dict["AzureBlob"]:
            a.delete(topic_str)
        #
        l = self.get_local()
        for group_str in self.storage_str_group_str_list_dict["Local"]:
#            l.delete_groups(group_str)
            pass
        for topic_str in self.storage_str_topic_str_list_dict["Local"]:
            l.delete(topic_str)
        #
        s = self.get_s3()
        for group_str in self.storage_str_group_str_list_dict["S3"]:
#            s.delete_groups(group_str)
            pass
        for topic_str in self.storage_str_topic_str_list_dict["S3"]:
            s.delete(topic_str)

    def create_test_topic_name(self, storage_obj):
        topic_str = f"test_topic_{get_millis()}"
        while True:
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

    def test_cp_azureblob_to_local(self):
        a = self.get_azureblob()
        l = self.get_local()
        #
        test_cp(self, a, l)

    def test_cp_azureblob_to_s3(self):
        a = self.get_azureblob()
        s = self.get_s3()
        #
        test_cp(self, a, s)

    def test_cp_local_to_azureblob(self):
        l = self.get_local()
        a = self.get_azureblob()
        #
        test_cp(self, l, a)

    def test_cp_local_to_s3(self):
        l = self.get_local()
        s = self.get_s3()
        #
        test_cp(self, l, s)

    def test_cp_s3_to_azureblob(self):
        s = self.get_s3()
        a = self.get_azureblob()
        #
        test_cp(self, s, a)

    def test_cp_s3_to_local(self):
        s = self.get_s3()
        l = self.get_local()
        #
        test_cp(self, s, l)

    # Cp from kafka storage to fs storage

    def test_cp_cluster_to_azureblob(self):
        c = Cluster("local")
        a = self.get_azureblob()
        #
        test_cp(self, c, a)

    def test_cp_cluster_to_local(self):
        c = Cluster("local")
        l = self.get_local()
        #
        test_cp(self, c, l)

    def test_cp_cluster_to_s3(self):
        c = Cluster("local")
        s = self.get_s3()
        #
        test_cp(self, c, s)

    def test_cp_restproxy_to_azureblob(self):
        r = RestProxy("local")
        a = self.get_azureblob()
        #
        test_cp(self, r, a)

    def test_cp_restproxy_to_local(self):
        r = RestProxy("local")
        l = self.get_local()
        #
        test_cp(self, r, l)

    def test_cp_restproxy_to_s3(self):
        r = RestProxy("local")
        s = self.get_s3()
        #
        test_cp(self, r, s)

    # Cp from fs storage to kafka storage

    def test_cp_azureblob_to_cluster(self):
        a = self.get_azureblob()
        c = Cluster("local")
        #
        test_cp(self, a, c)

    def test_cp_local_to_cluster(self):
        l = self.get_local()
        c = Cluster("local")
        #
        test_cp(self, l, c)

    def test_cp_s3_to_cluster(self):
        s = self.get_s3()
        c = Cluster("local")
        #
        test_cp(self, s, c)

    def test_cp_azureblob_to_restproxy(self):
        a = self.get_azureblob()
        r = RestProxy("local")
        #
        test_cp(self, a, r)

    def test_cp_local_to_restproxy(self):
        l = self.get_local()
        r = RestProxy("local")
        #
        test_cp(self, l, r)

    def test_cp_s3_to_restproxy(self):
        s = self.get_s3()
        r = RestProxy("local")
        #
        test_cp(self, s, r)

    #

    def test_cp_cluster_to_restproxy(self):
        c = Cluster("local")
        r = RestProxy("local")
        #
        test_cp(self, c, r)

    def test_cp_restproxy_to_cluster(self):
        r = RestProxy("local")
        c = Cluster("local")
        test_cp(self, r, c)

    # Diff AzureBlob and X

    def test_diff_azureblob_local(self):
        a = self.get_azureblob()
        l = self.get_local()
        #        
        test_diff(self, a, l)

    def test_diff_azureblob_s3(self):
        a = self.get_azureblob()
        s = self.get_s3()
        #        
        test_diff(self, a, s)

    def test_diff_azureblob_cluster(self):
        a = self.get_azureblob()
        c = Cluster("local")
        #        
        test_diff(self, a, c)

    def test_diff_azureblob_restproxy(self):
        a = self.get_azureblob()
        r = RestProxy("local")
        #        
        test_diff(self, a, r)

    # Diff Local and X

    def test_diff_local_azureblob(self):
        l = self.get_local()
        a = self.get_azureblob()
        #        
        test_diff(self, l, a)

    def test_diff_local_s3(self):
        l = self.get_local()
        s = self.get_s3()
        #        
        test_diff(self, l, s)

    def test_diff_local_cluster(self):
        l = self.get_local()
        c = Cluster("local")
        #        
        test_diff(self, l, c)

    def test_diff_local_restproxy(self):
        l = self.get_local()
        r = RestProxy("local")
        #        
        test_diff(self, l, r)

    # Diff S3 and X

    def test_diff_s3_azureblob(self):
        s = self.get_s3()
        a = self.get_azureblob()
        #        
        test_diff(self, s, s)

    def test_diff_s3_local(self):
        s = self.get_s3()
        l = self.get_local()
        #        
        test_diff(self, s, l)

    def test_diff_s3_cluster(self):
        s = self.get_s3()
        c = Cluster("local")
        #        
        test_diff(self, s, c)

    def test_diff_s3_restproxy(self):
        s = self.get_s3()
        r = RestProxy("local")
        #        
        test_diff(self, s, r)

    # Diff Cluster and X

    def test_diff_cluster_azureblob(self):
        c = Cluster("local")
        a = self.get_azureblob()
        #        
        test_diff(self, c, a)

    def test_diff_cluster_local(self):
        c = Cluster("local")
        l = self.get_local()
        #        
        test_diff(self, c, l)

    def test_diff_cluster_s3(self):
        c = Cluster("local")
        s = self.get_s3()
        #        
        test_diff(self, c, s)

    def test_diff_cluster_restproxy(self):
        c = Cluster("local")
        r = RestProxy("local")
        #        
        test_diff(self, c, r)

    # Diff RestProxy and X

    def test_diff_restproxy_azureblob(self):
        r = RestProxy("local")
        a = self.get_azureblob()
        #        
        test_diff(self, r, a)

    def test_diff_restproxy_local(self):
        r = RestProxy("local")
        l = self.get_local()
        #        
        test_diff(self, r, l)

    def test_diff_restproxy_s3(self):
        r = RestProxy("local")
        s = self.get_s3()
        #        
        test_diff(self, r, s)

    def test_diff_restproxy_cluster(self):
        r = RestProxy("local")
        c = Cluster("local")
        #        
        test_diff(self, r, c)

    # Files AzureBlob and X

    def test_from_to_file_azureblob_azureblob(self):
        a1 = self.get_azureblob()
        a2 = self.get_azureblob()
        #        
        test_from_to_file(self, a1, a2)

    def test_from_to_file_azureblob_local(self):
        a = self.get_azureblob()
        l = self.get_local()
        #        
        test_from_to_file(self, a, l)

    def test_from_to_file_azureblob_s3(self):
        a = self.get_azureblob()
        s = self.get_s3()
        #        
        test_from_to_file(self, a, s)

    # Files Local and X

    def test_from_to_file_local_azureblob(self):
        l = self.get_local()
        a = self.get_azureblob()
        #        
        test_from_to_file(self, l, a)

    def test_from_to_file_local_local(self):
        l1 = self.get_local()
        l2 = self.get_local()
        #        
        test_from_to_file(self, l1, l2)

    def test_from_to_file_local_s3(self):
        l = self.get_local()
        s = self.get_s3()
        #        
        test_from_to_file(self, l, s)

    # Files S3 and X

    def test_from_to_file_s3_azureblob(self):
        s = self.get_s3()
        a = self.get_azureblob()
        #        
        test_from_to_file(self, s, a)

    def test_from_to_file_s3_local(self):
        s = self.get_s3()
        l = self.get_local()
        #        
        test_from_to_file(self, s, l)

    def test_from_to_file_s3_s3(self):
        s1 = self.get_s3()
        s2 = self.get_s3()
        #        
        test_from_to_file(self, s1, s2)

    # Files Cluster and X

    def test_from_to_cluster_azureblob(self):
        c = Cluster("local")
        a = self.get_azureblob()
        #        
        test_from_to_file(self, c, a)

    def test_from_to_file_cluster_local(self):
        c = Cluster("local")
        l = self.get_local()
        #        
        test_from_to_file(self, c, l)

    def test_from_to_file_cluster_s3(self):
        c = Cluster("local")
        s = self.get_s3()
        #        
        test_from_to_file(self, c, s)

    # Files RestProxy and X

    def test_from_to_restproxy_azureblob(self):
        r = RestProxy("local")
        a = self.get_azureblob()
        #        
        test_from_to_file(self, r, a)

    def test_from_to_file_restproxy_local(self):
        r = RestProxy("local")
        l = self.get_local()
        #        
        test_from_to_file(self, r, l)

    def test_from_to_file_restproxy_s3(self):
        r = RestProxy("local")
        s = self.get_s3()
        #        
        test_from_to_file(self, r, s)

#

def test_cp(test_obj, storage1, storage2):
    partitions_int = 3
    # Create topic1 on storage1
    topic_str1 = test_obj.create_test_topic_name(storage1)
    storage1.create(topic_str1, partitions=partitions_int)
    random_int = random.randint(0, 2)
    type_str = ["avro", "protobuf", "jsonschema"][random_int]
    key_schema_str = test_obj.key_schema_str_list[random_int]
    value_schema_str = test_obj.value_schema_str_list[random_int]
    w = storage1.producer(topic_str1, key_type=type_str, value_type=type_str, key_schema=key_schema_str, value_schema=value_schema_str)
    #
    snack_dict_list = []
    for snack_dict in test_obj.snack_dict_list:
        snack_dict1 = snack_dict.copy()
        snack_dict1["name"] += "1"
        snack_dict2 = snack_dict.copy()
        snack_dict2["name"] += "2"
        snack_dict3 = snack_dict.copy()
        snack_dict3["name"] += "3"
        snack_dict_list += [snack_dict1, snack_dict2, snack_dict3]
    w.produce(snack_dict_list, key=[{"key": "1"}, {"key": "1"}, {"key": "1"}, {"key": "2"}, {"key": "2"}, {"key": "2"}, {"key": "3"}, {"key": "3"}, {"key": "3"}], headers=test_obj.headers_str_bytes_tuple_list)
    w.close()
    # Carbon copy topic1 on storage1 to topic2 on storage2 as bytes.
    topic_str2 = test_obj.create_test_topic_name(storage2)
    storage2.create(topic_str2, partitions=partitions_int)
    #
    group_str1 = test_obj.create_test_group_name(storage1)
    produce_batch_size_int1 = random.randint(1, 3*3)
    if storage1.__class__.__name__ == "RestProxy":
        # Need to use the native Kafka API here since the RestProxy V2 consumer does not support timestamps.
        c = Cluster("local")
        (consume_n_int1, written_n_int1) = c.cp(topic_str1, storage2, topic_str2, group=group_str1, source_key_type="bytes", source_value_type="bytes", target_key_type="bytes", target_value_type="bytes", produce_batch_size=produce_batch_size_int1, n=3*3, keep_timestamps=True, keep_partitions=True)
    else:
        (consume_n_int1, written_n_int1) = storage1.cp(topic_str1, storage2, topic_str2, group=group_str1, source_key_type="bytes", source_value_type="bytes", target_key_type="bytes", target_value_type="bytes", produce_batch_size=produce_batch_size_int1, n=3*3, keep_timestamps=True, keep_partitions=True)
    #
    test_obj.assertEqual(3*3, consume_n_int1)
    test_obj.assertEqual(3*3, written_n_int1)
    #
    # Carbon copy topic2 on storage2 back to topic3 on storage1 as bytes.
    topic_str3 = test_obj.create_test_topic_name(storage1)
    storage1.create(topic_str3, partitions=partitions_int)
    #
    group_str2 = test_obj.create_test_group_name(storage2)
    produce_batch_size_int2 = random.randint(1, 3*3)
    if storage2.__class__.__name__ == "RestProxy":
        # Need to use the native Kafka API here since the RestProxy V2 consumer does not support timestamps.
        c = Cluster("local")
        (consume_n_int2, written_n_int2) = c.cp(topic_str2, storage1, topic_str3, group=group_str2, source_key_type="bytes", source_value_type="bytes", target_key_type="bytes", target_value_type="bytes", produce_batch_size=produce_batch_size_int2, n=3*3, keep_timestamps=True, keep_partitions=True)
    else:
        (consume_n_int2, written_n_int2) = storage2.cp(topic_str2, storage1, topic_str3, group=group_str2, source_key_type="bytes", source_value_type="bytes", target_key_type="bytes", target_value_type="bytes", produce_batch_size=produce_batch_size_int2, n=3*3, keep_timestamps=True, keep_partitions=True)
    #
    test_obj.assertEqual(3*3, consume_n_int2)
    test_obj.assertEqual(3*3, written_n_int2)
    #
    # Have the partitions been carbon copied properly?
    storage1_topic1_watermarks_dict = storage1.watermarks(topic_str1)
    storage2_topic2_watermarks_dict = storage2.watermarks(topic_str2)
    storage1_topic3_watermarks_dict = storage1.watermarks(topic_str3)
    test_obj.assertEqual(list(storage1_topic1_watermarks_dict.values()), list(storage2_topic2_watermarks_dict.values()))
    test_obj.assertEqual(list(storage2_topic2_watermarks_dict.values()), list(storage1_topic3_watermarks_dict.values()))
    #
    # Have the timestamps been carbon copied properly?
    group_str3 = test_obj.create_test_group_name(storage1)
    if storage1.__class__.__name__ == "RestProxy":
        # Need to use the native Kafka API here since the RestProxy V2 consumer does not support timestamps.
        c = Cluster("local")
        message_dict_list1 = c.cat(topic_str1, group=group_str3, type=type_str, n=3*3)
    else:
        message_dict_list1 = storage1.cat(topic_str1, group=group_str3, type=type_str, n=3*3)
    #
    test_obj.assertEqual(3*3, len(message_dict_list1))
    #
    group_str4 = test_obj.create_test_group_name(storage1)
    if storage1.__class__.__name__ == "RestProxy":
        # Need to use the native Kafka API here since the RestProxy V2 consumer does not support timestamps.
        c = Cluster("local")
        message_dict_list2 = c.cat(topic_str3, group=group_str4, type=type_str, n=3*3)
    else:
        message_dict_list2 = storage1.cat(topic_str3, group=group_str4, type=type_str, n=3*3)
    #
    test_obj.assertEqual(3*3, len(message_dict_list2))
    #
    storage1_topic1_timestamp_set = set([message_dict["timestamp"] for message_dict in message_dict_list1])
    storage1_topic3_timestamp_set = set([message_dict["timestamp"] for message_dict in message_dict_list2])
    test_obj.assertEqual(storage1_topic1_timestamp_set, storage1_topic3_timestamp_set)

    # Copy topic3 on storage1 to topic5 on storage2 as json and do a tiny mapping.
    topic_str5 = test_obj.create_test_topic_name(storage2)
    storage2.create(topic_str5, partitions=partitions_int)
    #
    def map_ish(message_dict):
        message_dict["value"]["colour"] += "ish"
        return message_dict
    #

    group_str5 = test_obj.create_test_group_name(storage1)
    produce_batch_size_int3 = random.randint(1, 3*3)
    (consume_n_int3, written_n_int3) = storage1.cp(topic_str3, storage2, topic_str5, group=group_str5, source_type=type_str, target_type="json", produce_batch_size=produce_batch_size_int3, map_function=map_ish, n=3*3)
    #
    test_obj.assertEqual(3*3, consume_n_int3)
    test_obj.assertEqual(3*3, written_n_int3)
    #
    group_str6 = test_obj.create_test_group_name(storage1)
    message_dict_list3 = storage2.cat(topic_str5, group=group_str6, type="json", n=3*3)
    test_obj.assertEqual(3*3, len(message_dict_list3))
    #
    # Has the mapping been done properly?
    for message_dict in message_dict_list3:
        test_obj.assertTrue(message_dict["value"]["colour"].endswith("ish"))
    #
    # Has the order of the snacks been kept intact after all that copying?
    if storage1.__class__.__name__ != "RestProxy" and storage2.__class__.__name__ != "RestProxy":
        for i in range(3):
            j0 = next(j for j, message_dict in enumerate(message_dict_list3) if message_dict["value"]["name"] == snack_dict_list[3*i]["name"])
            j1 = next(j for j, message_dict in enumerate(message_dict_list3) if message_dict["value"]["name"] == snack_dict_list[3*i+1]["name"])
            j2 = next(j for j, message_dict in enumerate(message_dict_list3) if message_dict["value"]["name"] == snack_dict_list[3*i+2]["name"])
            #
            test_obj.assertEqual(message_dict_list3[j0]["partition"], message_dict_list3[j1]["partition"])
            test_obj.assertEqual(message_dict_list3[j1]["partition"], message_dict_list3[j2]["partition"])
            #
            test_obj.assertLess(message_dict_list3[j0]["offset"], message_dict_list3[j1]["offset"])
            test_obj.assertLess(message_dict_list3[j1]["offset"], message_dict_list3[j2]["offset"])

#

def test_diff(test_obj, storage1, storage2):
    topic_str1 = test_obj.create_test_topic_name(storage1)
    storage1.create(topic_str1)
    w1 = storage1.producer(topic_str1, type="str")
    w1.produce(test_obj.snack_str_list)
    w1.close()
    #
    topic_str2 = test_obj.create_test_topic_name(storage2)
    storage2.create(topic_str2)
    w2 = storage2.producer(topic_str2, type="str")
    w2.produce(test_obj.snack_ish_dict_list)
    w2.close()
    #
    group_str1 = test_obj.create_test_group_name(storage1)
    time.sleep(0.1)
    group_str2 = test_obj.create_test_group_name(storage2)
    #
    (message_dict_message_dict_tuple_list, message_counter_int1, message_counter_int2) = storage1.diff(topic_str1, storage2, topic_str2, group1=group_str1, group2=group_str2, type1="json", type2="json", n=3, consume_batch_size=3)
    test_obj.assertEqual(3, len(message_dict_message_dict_tuple_list))
    test_obj.assertEqual(3, message_counter_int1)
    test_obj.assertEqual(3, message_counter_int2)

# Files

def test_from_to_file(test_obj, storage1, storage2):
    if storage2.__class__.__name__ in ["AzureBlob", "Local", "S3"]:
        suffix_str_list = [".csv", ".feather", ".json", ".orc", ".parquet", ".xlsx", ".xml"]
        random_int = random.randint(0, len(suffix_str_list) - 1)
        suffix_str = suffix_str_list[random_int]
        #
        topic_str1 = test_obj.create_test_topic_name(storage1)
        storage1.create(topic_str1)
        producer = storage1.producer(topic_str1, value_type="json")
        producer.produce(test_obj.snack_str_list)
        producer.close()
        #
        group_str1 = test_obj.create_test_group_name(storage1)
        file_str = f"{topic_str1}{suffix_str}"
        n_int1 = storage1.topic_to_file(topic_str1, storage2, file_str, group=group_str1, type="json")
        test_obj.assertTrue(n_int1, 3)
        #
        topic_str2 = test_obj.create_test_topic_name(storage1)
        storage1.create(topic_str2)
        storage2.file_to_topic(file_str, storage1, topic_str2)
        #
        group_str2 = test_obj.create_test_group_name(storage1)
        message_dict_list = storage1.cat(topic_str2, group=group_str2, n=3, type="json")
        test_obj.assertEqual(3, len(message_dict_list))
        test_obj.assertEqual(500.0, message_dict_list[0]["value"]["calories"])
        test_obj.assertEqual(260.0, message_dict_list[1]["value"]["calories"])
        test_obj.assertEqual(80.0, message_dict_list[2]["value"]["calories"])
        #
        storage2.admin.delete_file(storage2.admin.get_abs_path_str(f"files/{file_str}"))
