import os

from kafi.storage_producer import StorageProducer
from kafi.helpers import get_millis, default_partitioner

# Constants

CURRENT_TIME = 0
TIMESTAMP_CREATE_TIME = 1

#

class FSProducer(StorageProducer):
    def __init__(self, fs_obj, topic, **kwargs):
        # The default partitioner function for the FSProducer is the default partitioner.
        self.partitioner_fun = kwargs["partitioner_fun"] if "partitioner_fun" in kwargs else default_partitioner
        self.projection_fun = kwargs["projection_fun"] if "projection_fun" in kwargs else lambda x: x["key"]
        #
        super().__init__(fs_obj, topic, **kwargs)
        #
        if not fs_obj.exists(self.topic_str):
            fs_obj.create(self.topic_str)

    #

    def produce_impl(self, m_list, **kwargs):
        partition_int_offsets_tuple_dict = self.storage_obj.watermarks(self.topic_str)[self.topic_str]
        last_offsets_dict = {partition_int: offsets_tuple[1] for partition_int, offsets_tuple in partition_int_offsets_tuple_dict.items()}
        #
        partition_int_m_list_dict = {partition_int: [] for partition_int in range(self.partitions_int)}
        partition_int_offset_counter_int_dict = {partition_int: last_offset_int if last_offset_int > 0 else 0 for partition_int, last_offset_int in last_offsets_dict.items()}
        #
        counter_int = 0
        for m in m_list:
            timestamp = m["timestamp"]
            if timestamp == CURRENT_TIME:
                timestamp = (TIMESTAMP_CREATE_TIME, get_millis())
            #
            partition_int = self.partitioner_fun(m, counter_int, self.partitions_int, self.projection_fun)
            #
            m = {"topic": self.topic_str,
                            "value": m["value"],
                            "key": m["key"],
                            "timestamp": timestamp,
                            "headers": m["headers"],
                            "partition": partition_int,
                            "offset": partition_int_offset_counter_int_dict[partition_int]}
            #
            partition_int_m_list_dict[partition_int].append(m)
            #
            partition_int_offset_counter_int_dict[partition_int] += 1
        #
        topic_abs_dir_str = self.storage_obj.admin.get_topic_abs_path_str(self.topic_str)
        for partition_int, m_list in partition_int_m_list_dict.items():
            if len(m_list) > 0:
                start_timestamp_int = m_list[0]["timestamp"][1]
                end_timestamp_int = m_list[-1]["timestamp"][1]
                #
                messages_bytes = b""
                for m in m_list:
                    start_offset_int = last_offsets_dict[partition_int]
                    end_offset_int = start_offset_int + len(m_list) - 1
                    #
                    abs_path_file_str = os.path.join(topic_abs_dir_str, "partitions", f"{partition_int:09},{start_offset_int:021},{end_offset_int:021},{start_timestamp_int},{end_timestamp_int}")
                    #
                    message_bytes = str(m).encode("utf-8") + b"\n"
                    #
                    messages_bytes += message_bytes
                #
                self.storage_obj.admin.write_bytes(abs_path_file_str, messages_bytes)
