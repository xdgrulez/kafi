from kafi.kafka.kafka_producer import KafkaProducer
from kafi.helpers import post, is_base64_encoded, base64_encode

import datetime
import json

# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1
TIMESTAMP_CREATE_TIME = 1

#

class RestProxyProducer(KafkaProducer):
    def __init__(self, restproxy_obj, topic, **kwargs):
        super().__init__(restproxy_obj, topic, **kwargs)
        #
        self.cluster_id_str = restproxy_obj.cluster_id_str

    #

    def close(self):
        pass

    #

    def produce(self, value, **kwargs):
        (rest_proxy_url_str, auth_str_tuple) = self.storage_obj.get_url_str_auth_str_tuple_tuple()
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{self.topic_str}/records"
        #
        key = kwargs["key"] if "key" in kwargs else None
        partition = kwargs["partition"] if "partition" in kwargs else RD_KAFKA_PARTITION_UA
        timestamp = kwargs["timestamp"] if "timestamp" in kwargs else CURRENT_TIME
        headers = kwargs["headers"] if "headers" in kwargs else None
        #
        value_list = value if isinstance(value, list) else [value]
        #
        key_list = key if isinstance(key, list) else [key for _ in value_list]
        #
        partition_int_list = partition if isinstance(partition, list) else [partition for _ in value_list]
        #
        timestamp_list = timestamp if isinstance(timestamp, list) else [timestamp for _ in value_list]
        #
        headers_list = headers if isinstance(headers, list) and all(self.storage_obj.is_headers(headers1) for headers1 in headers) and len(headers) == len(value_list) else [headers for _ in value_list]
        headers_str_bytes_tuple_list_list = [self.storage_obj.headers_to_headers_str_bytes_tuple_list(headers) for headers in headers_list]
        #
        payload_dict_list = []
        for value, key, timestamp, headers_str_bytes_tuple_list, partition_int in zip(value_list, key_list, timestamp_list, headers_str_bytes_tuple_list_list, partition_int_list):
            headers_dict = {"Content-Type": "application/json", "Transfer-Encoding": "chunked"}
            #
            if self.value_type_str.lower() == "json":
                type_str = "JSON"
                if value is not None and not isinstance(value, dict):
                    value = json.loads(value)
            elif self.value_type_str.lower() == "avro":
                type_str = "AVRO"
                if value is not None and not isinstance(value, dict):
                    value = json.loads(value)
            elif self.value_type_str.lower() in ["pb", "protobuf"]:
                type_str = "PROTOBUF"
                if value is not None and not isinstance(value, dict):
                    value = json.loads(value)
            elif self.value_type_str.lower() in ["jsonschema", "json_sr"]:
                type_str = "JSONSCHEMA"
                if value is not None and not isinstance(value, dict):
                    value = json.loads(value)
            else:
                type_str = "BINARY"
                if not is_base64_encoded(value):
                    value_bytes = base64_encode(value)
                    value = value_bytes.decode()
            #
            if self.value_schema_id_int is not None:
                payload_dict = {"value": {"schema_id": self.value_schema_id_int, "data": value}}
            elif self.value_schema_str_or_dict is not None:
                payload_dict = {"value": {"type": type_str, "schema": self.value_schema_str_or_dict, "data": value}}
            else:
                payload_dict = {"value": {"type": type_str, "data": value}}
            #
            if key is not None:
                if self.key_type_str.lower() == "json":
                    type_str = "JSON"
                    if not isinstance(key, dict):
                        key = json.loads(key)
                elif self.key_type_str.lower() == "avro":
                    type_str = "AVRO"
                    if not isinstance(key, dict):
                        key = json.loads(key)
                elif self.key_type_str.lower() in ["pb", "protobuf"]:
                    type_str = "PROTOBUF"
                    if not isinstance(key, dict):
                        key = json.loads(key)
                elif self.key_type_str.lower() in ["jsonschema", "json_sr"]:
                    type_str = "JSONSCHEMA"
                    if not isinstance(key, dict):
                        key = json.loads(key)
                else:
                    type_str = "BINARY"
                    if not is_base64_encoded(key):
                        key_bytes = base64_encode(key)
                        key = key_bytes.decode()
                #
                if self.key_schema_id_int is not None:
                    payload_dict["key"] = {"schema_id": self.key_schema_id_int, "data": key}
                elif self.key_schema_str_or_dict is not None:
                    payload_dict["key"] = {"type": type_str, "schema": self.key_schema_str_or_dict, "data": key}
                else:
                    payload_dict["key"] = {"type": type_str, "data": key}
            #
            if self.keep_timestamps_bool or timestamp is not None:
                if isinstance(timestamp, tuple):
                    timestamp_str = datetime.datetime.fromtimestamp(timestamp[1]/1000.0, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    payload_dict["timestamp"] = timestamp_str
            #
            if headers_str_bytes_tuple_list is not None:
                payload_dict["headers"] = [{"name": headers_str_bytes_tuple[0], "value": base64_encode(headers_str_bytes_tuple[1]).decode("utf-8")} for headers_str_bytes_tuple in headers_str_bytes_tuple_list]
            #
            if partition_int is not None and partition_int != RD_KAFKA_PARTITION_UA:
                payload_dict["partition_id"] = partition_int
            #
            payload_dict_list.append(bytes(json.dumps(payload_dict, default=str), "utf-8"))
        #
        #payload_dict_generator = (payload_dict for payload_dict in payload_dict_list)
        def g():
            for x in payload_dict_list:
                yield x
        payload_dict_generator = g()
        #
        post(url_str, headers_dict, payload_dict_generator, auth_str_tuple=auth_str_tuple, retries_int=self.storage_obj.requests_num_retries(), debug_bool=self.storage_obj.verbose() >= 2)
        #
        self.written_counter_int += len(payload_dict_list)
        #
        return key_list, value_list
