from kafi.kafka.kafka_consumer import KafkaConsumer

from kafi.helpers import get, delete, post, base64_decode

#

class RestProxyConsumer(KafkaConsumer):
    def __init__(self, restproxy_obj, *topics, **kwargs):
        super().__init__(restproxy_obj, *topics, **kwargs)
        #
        self.cluster_id_str = restproxy_obj.cluster_id_str
        #
        # Consumer Config
        #
        if "fetch.min.bytes" not in self.consumer_config_dict:
            self.consumer_config_dict["fetch.min.bytes"] = restproxy_obj.fetch_min_bytes()
        if "consumer.request.timeout.ms" not in self.consumer_config_dict:
            self.consumer_config_dict["consumer.request.timeout.ms"] = restproxy_obj.consumer_request_timeout_ms()
        if "auto.commit.enable" not in self.consumer_config_dict:
            self.consumer_config_dict["auto.commit.enable"] = self.enable_auto_commit_bool
        #
        # Instance ID
        #
        self.instance_id_str = None
        #
        self.subscribe()

    #

    def subscribe(self):
        (rest_proxy_url_str, auth_str_tuple) = self.storage_obj.get_url_str_auth_str_tuple_tuple()
        #
        url_str1 = f"{rest_proxy_url_str}/consumers/{self.group_str}"
        headers_dict1 = {"Content-Type": "application/vnd.kafka.v2+json"}
        #
        value_type_str = self.topic_str_key_type_str_dict[self.topic_str_list[0]]
        if value_type_str.lower() == "json":
            type_str = "JSON"
        elif value_type_str.lower() == "avro":
            type_str = "AVRO"
        elif value_type_str.lower() in ["pb", "protobuf"]:
            type_str = "PROTOBUF"
        elif value_type_str.lower() in ["jsonschema", "json_sr"]:
            type_str = "JSONSCHEMA"
        else: # str or bytes
            type_str = "BINARY"
        #
        payload_dict1 = {"format": type_str}
        payload_dict1.update(self.consumer_config_dict)
        response_dict1 = post(url_str1, headers_dict1, payload_dict1, auth_str_tuple=auth_str_tuple, retries_int=self.storage_obj.requests_num_retries(), debug_bool=self.storage_obj.verbose() >= 2)
        self.instance_id_str = response_dict1["instance_id"]
        #
# {
#   "offsets": [
#     {
#       "topic": "test",
#       "partition": 0,
#       "offset": 20
#     },
#     {
#       "topic": "test",
#       "partition": 1,
#       "offset": 30
#     }
#   ]
# }
        if self.topic_str_start_offsets_dict_dict is not None:
            self.commit(self.topic_str_start_offsets_dict_dict)
            # rest_proxy_offset_dict_list = [{"topic": topic_str, "partition": partition_int, "offset": offset_int} for topic_str, start_offsets_dict in self.topic_str_start_offsets_dict_dict.items() for partition_int, offset_int in start_offsets_dict.items()]
            # url_str2 = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/offsets"
            # headers_dict2 = {"Content-Type": "application/vnd.kafka.v2+json"}
            # payload_dict2 = {"offsets": rest_proxy_offset_dict_list}
            # post(url_str2, headers_dict2, payload_dict2, auth_str_tuple=auth_str_tuple, retries_int=self.storage_obj.requests_num_retries(), debug_bool=self.storage_obj.verbose() >= 2)
        #
        url_str3 = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/subscription"
        headers_dict3 = {"Content-Type": "application/vnd.kafka.v2+json"}
        payload_dict3 = {"topics": self.topic_str_list}
        post(url_str3, headers_dict3, payload_dict3, auth_str_tuple=auth_str_tuple, retries_int=self.storage_obj.requests_num_retries(), debug_bool=self.storage_obj.verbose() >= 2)
        #
        return self.topic_str_list, self.group_str
    
    def unsubscribe(self):
        (rest_proxy_url_str, auth_str_tuple) = self.storage_obj.get_url_str_auth_str_tuple_tuple()
        #
        url_str = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/subscription"
        headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
        delete(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries_int=self.storage_obj.requests_num_retries(), debug_bool=self.storage_obj.verbose() >= 2)
        #
        return self.topic_str_list, self.group_str

    def close(self):
        self.unsubscribe()
        #
        (rest_proxy_url_str, auth_str_tuple) = self.storage_obj.get_url_str_auth_str_tuple_tuple()
        #
        url_str = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}"
        headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
        delete(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries_int=self.storage_obj.requests_num_retries(), debug_bool=self.storage_obj.verbose() >= 2)
        #
        return self.topic_str_list, self.group_str

    #

    def consume_impl(self, **kwargs):
        timeout_int = kwargs["timeout"] if "timeout" in kwargs else None
        max_bytes_int = kwargs["max_bytes"] if "max_bytes" in kwargs else None 
        #
        (rest_proxy_url_str, auth_str_tuple) = self.storage_obj.get_url_str_auth_str_tuple_tuple()
        #
        if timeout_int is None:
            timeout_int = self.consumer_config_dict["consumer.request.timeout.ms"]
        #
        if max_bytes_int is None:
            max_bytes_int = 67108864
        #
        key_type_str = self.topic_str_key_type_str_dict[self.topic_str_list[0]]
        value_type_str = self.topic_str_value_type_str_dict[self.topic_str_list[0]]
        if value_type_str.lower() == "json":
            type_str = "json"
        elif value_type_str.lower() == "avro":
            type_str = "avro"
        elif value_type_str.lower() in ["pb", "protobuf"]:
            type_str = "protobuf"
        elif value_type_str.lower() in ["jsonschema", "json_sr"]:
            type_str = "jsonschema"
        else:
            type_str = "binary"
        #
        url_str = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/records?timeout={timeout_int}&max_bytes={max_bytes_int}"
        headers_dict = {"Accept": f"application/vnd.kafka.{type_str}.v2+json"}
        #
        def decode(key_or_value, key_bool=False):
            if key_or_value is None:
                return None
            elif type_str in ["json", "avro", "protobuf", "jsonschema"]:
                return key_or_value
            elif type_str == "binary":
                decoded_bytes = base64_decode(key_or_value)
                type_str1 = key_type_str if key_bool else value_type_str
                if type_str1 == "bytes":
                    return decoded_bytes
                elif type_str1 in ["str", "string"]:
                    return decoded_bytes.decode(encoding="utf-8")

        #
        message_dict_list = []
        for _ in range(0, self.storage_obj.consume_num_attempts()):
            response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries_int=self.storage_obj.requests_num_retries(), debug_bool=self.storage_obj.verbose() >= 2)
            #
            message_dict_list += [{"headers": None, "topic": rest_message_dict["topic"], "partition": rest_message_dict["partition"], "offset": rest_message_dict["offset"], "timestamp": None, "key": decode(rest_message_dict["key"], True), "value": decode(rest_message_dict["value"], False)} for rest_message_dict in response_dict]
        #
        return message_dict_list

    #

    def commit(self, offsets=None, **kwargs):
        offsets_dict = offsets
        #
        (rest_proxy_url_str, auth_str_tuple) = self.storage_obj.get_url_str_auth_str_tuple_tuple()
        #
        headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
        #
        if offsets is None:
            payload_dict = None
        else:
            rest_offsets_dict_list = offsets_dict_to_rest_offsets_dict_list(offsets_dict, self.topic_str_list)
            payload_dict = {"offsets": rest_offsets_dict_list}
        #
        url_str = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/offsets"
        post(url_str, headers_dict, payload_dict, auth_str_tuple=auth_str_tuple, retries_int=self.storage_obj.requests_num_retries(), debug_bool=self.storage_obj.verbose() >= 2)
        #
        return {}

    def offsets(self):
        (rest_proxy_url_str, auth_str_tuple) = self.storage_obj.get_url_str_auth_str_tuple_tuple()
        #
        url_str1 = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/assignments"
        headers_dict1 = {"Accept": "application/vnd.kafka.v2+json"}
        response_dict1 = get(url_str1, headers_dict1, auth_str_tuple=auth_str_tuple, retries_int=self.storage_obj.requests_num_retries(), debug_bool=self.storage_obj.verbose() >= 2)
        #
        url_str2 = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/offsets"
        headers_dict2 = {"Content-Type": "application/vnd.kafka.v2+json"}
        response_dict2 = get(url_str2, headers_dict2, response_dict1, auth_str_tuple=auth_str_tuple, retries_int=self.storage_obj.requests_num_retries(), debug_bool=self.storage_obj.verbose() >= 2)
        #
        topic_str_offsets_dict_dict = {}
        for rest_topic_partition_offset_dict in response_dict2["offsets"]:
            topic_str = rest_topic_partition_offset_dict["topic"]
            partition_int = rest_topic_partition_offset_dict["partition"]
            offset_int = rest_topic_partition_offset_dict["offset"]
            #
            if topic_str not in topic_str_offsets_dict_dict:
                topic_str_offsets_dict_dict[topic_str] = {}
            #
            if partition_int not in topic_str_offsets_dict_dict[topic_str]:
                topic_str_offsets_dict_dict[topic_str] = {}
            #
            topic_str_offsets_dict_dict[topic_str][partition_int] = offset_int
        #
        return topic_str_offsets_dict_dict

#

def offsets_dict_to_rest_offsets_dict_list(offsets_dict, topic_str_list):
    str_or_int = list(offsets_dict.keys())[0]
    if isinstance(str_or_int, str):
        topic_str_offsets_dict_dict = offsets_dict
    elif isinstance(str_or_int, int):
        topic_str_offsets_dict_dict = {topic_str: offsets_dict for topic_str in topic_str_list}
    #
    rest_offsets_dict_list = [{"topic": topic_str, "partition": partition_int, "offset": offset_int - 1} for topic_str, offsets_dict in topic_str_offsets_dict_dict.items() for partition_int, offset_int in offsets_dict.items() if offset_int > 0]
    # rest_offsets_dict_list = [{"topic": topic_str, "partition": partition_int, "offset": offset_int - 1 if offset_int > 0 else 0} for topic_str, offsets_dict in topic_str_offsets_dict_dict.items() for partition_int, offset_int in offsets_dict.items() if offset_int > 0]
    return rest_offsets_dict_list
