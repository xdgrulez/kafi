import json
import os

from kafi.storage_writer import StorageWriter
from kafi.helpers import get_millis, split_list_into_sublists, base64_encode

# Constants

CURRENT_TIME = 0
TIMESTAMP_CREATE_TIME=1

#

class FSWriter(StorageWriter):
    def __init__(self, fs_obj, topic, **kwargs):
        super().__init__(fs_obj, topic, **kwargs)
        #
        if not fs_obj.exists(self.topic_str):
            fs_obj.admin.create(self.topic_str)

    #

    def write(self, value, **kwargs):
        def serialize(payload, key_bool):
            type_str = self.key_type_str if key_bool else self.value_type_str
            #
            if not type_str.lower() in ["json", "str", "bytes"]:
                raise Exception("Only json, str or bytes supported.")
            #
            if isinstance(payload, dict):
                payload_bytes = json.dumps(payload).encode("utf-8")
            elif isinstance(payload, str):
                payload_bytes = payload.encode("utf-8")
            else:
                payload_bytes = payload
            #
            return payload_bytes
        #        

        partitions_int = self.storage_obj.admin.get_partitions(self.topic_str)
        #
        topic_abs_dir_str = self.storage_obj.admin.get_topic_abs_dir_str(self.topic_str)
        #
        partition_int_offsets_tuple_dict = self.storage_obj.admin.watermarks(self.topic_str)[self.topic_str]
        partition_int_last_offset_int_dict = {partition_int: offsets_tuple[1] for partition_int, offsets_tuple in partition_int_offsets_tuple_dict.items()}
        #
        write_batch_size_int = kwargs["write_batch_size"] if "write_batch_size" in kwargs else self.storage_obj.write_batch_size()
        message_separator_bytes = self.storage_obj.admin.get_message_separator(self.topic_str)
        #
        keep_partitions_bool = "keep_partitions" in kwargs and kwargs["keep_partitions"]
        keep_timestamps_bool = "keep_timestamps" in kwargs and kwargs["keep_timestamps"]
        key = kwargs["key"] if "key" in kwargs else None
        timestamp = kwargs["timestamp"] if "timestamp" in kwargs and kwargs["timestamp"] is not None else CURRENT_TIME
        headers = kwargs["headers"] if "headers" in kwargs else None
        partitions = kwargs["partition"] if "partition" in kwargs else None
        #
        value_list = value if isinstance(value, list) else [value]
        #
        key_list = key if isinstance(key, list) else [key for _ in value_list]
        #
        timestamp_list = timestamp if isinstance(timestamp, list) else [timestamp for _ in value_list]
        headers_list = headers if isinstance(headers, list) and len(headers) == len(value_list) else [headers for _ in value_list]
        headers_str_bytes_tuple_list_list = [self.storage_obj.headers_to_headers_str_bytes_tuple_list(headers) for headers in headers_list]
        partition_int_list = partitions if isinstance(partitions, list) else [partitions for _ in value_list]
        #
        partition_int_message_bytes_list_dict = {partition_int: [] for partition_int in range(partitions_int)}
        round_robin_counter_int = 0
        partition_int_offset_counter_int_dict = {partition_int: last_offset_int if last_offset_int > 0 else 0 for partition_int, last_offset_int in partition_int_last_offset_int_dict.items()}
        for value, key, timestamp, headers_str_bytes_tuple_list, partition_int in zip(value_list, key_list, timestamp_list, headers_str_bytes_tuple_list_list, partition_int_list):
            value_bytes = serialize(value, False)
            key_bytes = serialize(key, True)
            #
            if keep_partitions_bool:
                target_partition_int = partition_int
            else:
                target_partition_int = None
            #
            if target_partition_int is None:
                if key is None:
                    target_partition_int = round_robin_counter_int
                    if round_robin_counter_int == partitions_int - 1:
                        round_robin_counter_int = 0
                    else:
                        round_robin_counter_int += 1
                else:
                    target_partition_int = hash(key_bytes) % partitions_int
            #
            if timestamp == CURRENT_TIME:
                if not keep_timestamps_bool:
                    timestamp = (TIMESTAMP_CREATE_TIME, get_millis())
            if not isinstance(timestamp, tuple):
                timestamp = (TIMESTAMP_CREATE_TIME, timestamp)
            #
            message_bytes = str({"value": value_bytes, "key": key_bytes, "timestamp": timestamp, "headers": headers_str_bytes_tuple_list, "partition": target_partition_int, "offset": partition_int_offset_counter_int_dict[target_partition_int]}).encode("utf-8") + message_separator_bytes
            #
            partition_int_message_bytes_list_dict[target_partition_int].append(message_bytes)
            #
            partition_int_offset_counter_int_dict[target_partition_int] += 1
        #
        partition_int_message_bytes_list_list_dict = {partition_int: split_list_into_sublists(message_bytes_list, write_batch_size_int) for partition_int, message_bytes_list in partition_int_message_bytes_list_dict.items()}
        #
        for partition_int, message_bytes_list_list in partition_int_message_bytes_list_list_dict.items():
            batch_counter_int = 0
            for message_bytes_list in message_bytes_list_list:
                joined_message_bytes = b"".join(message_bytes_list)
                #
                start_offset_int = partition_int_last_offset_int_dict[partition_int] + batch_counter_int * write_batch_size_int
                abs_path_file_str = os.path.join(topic_abs_dir_str, f"partition,{partition_int:09},{start_offset_int:021}")
                self.write_bytes(abs_path_file_str, joined_message_bytes)
                #
                batch_counter_int += 1
