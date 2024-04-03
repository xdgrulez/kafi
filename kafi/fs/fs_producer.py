import os

from kafi.storage_producer import StorageProducer
from kafi.helpers import get_millis

# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1
TIMESTAMP_CREATE_TIME = 1

#

class FSProducer(StorageProducer):
    def __init__(self, fs_obj, topic, **kwargs):
        super().__init__(fs_obj, topic, **kwargs)
        #
        if not fs_obj.exists(self.topic_str):
            fs_obj.create(self.topic_str)

    #

    def produce(self, value, **kwargs):
        partition_int_offsets_tuple_dict = self.storage_obj.watermarks(self.topic_str)[self.topic_str]
        last_offsets_dict = {partition_int: offsets_tuple[1] for partition_int, offsets_tuple in partition_int_offsets_tuple_dict.items()}
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
        partitions_int = self.storage_obj.admin.get_partitions(self.topic_str)
        partition_int_message_dict_list_dict = {partition_int: [] for partition_int in range(partitions_int)}
        round_robin_counter_int = 0
        partition_int_offset_counter_int_dict = {partition_int: last_offset_int if last_offset_int > 0 else 0 for partition_int, last_offset_int in last_offsets_dict.items()}
        #
        for value, key, timestamp, headers_str_bytes_tuple_list, partition_int in zip(value_list, key_list, timestamp_list, headers_str_bytes_tuple_list_list, partition_int_list):
            if partition_int is RD_KAFKA_PARTITION_UA:
                if key is None:
                    partition_int = round_robin_counter_int
                    if round_robin_counter_int == partitions_int - 1:
                        round_robin_counter_int = 0
                    else:
                        round_robin_counter_int += 1
                else:
                    partition_int = hash(str(key)) % partitions_int
            #
            if timestamp == CURRENT_TIME:
                timestamp = (TIMESTAMP_CREATE_TIME, get_millis())
            #
            message_dict = {"topic": self.topic_str, "value": value, "key": key, "timestamp": timestamp, "headers": headers_str_bytes_tuple_list, "partition": partition_int, "offset": partition_int_offset_counter_int_dict[partition_int]}
            #
            partition_int_message_dict_list_dict[partition_int].append(message_dict)
            #
            partition_int_offset_counter_int_dict[partition_int] += 1
        #
        topic_abs_dir_str = self.storage_obj.admin.get_topic_abs_path_str(self.topic_str)
        for partition_int, message_dict_list in partition_int_message_dict_list_dict.items():
            if len(message_dict_list) > 0:
                start_timestamp_int = message_dict_list[0]["timestamp"][1]
                end_timestamp_int = message_dict_list[-1]["timestamp"][1]
                #
                messages_bytes = b""
                for message_dict in message_dict_list:
                    message_dict["key"] = self.serialize(message_dict["key"], True)
                    message_dict["value"] = self.serialize(message_dict["value"], False)
                    #
                    start_offset_int = last_offsets_dict[partition_int]
                    end_offset_int = start_offset_int + len(message_dict_list) - 1
                    #
                    abs_path_file_str = os.path.join(topic_abs_dir_str, "partitions", f"{partition_int:09},{start_offset_int:021},{end_offset_int:021},{start_timestamp_int},{end_timestamp_int}")
                    #
                    message_bytes = str(message_dict).encode("utf-8") + b"\n"
                    #
                    messages_bytes += message_bytes
                #
                self.storage_obj.admin.write_bytes(abs_path_file_str, messages_bytes)
