import json
import pandas as pd

from kafi.helpers import is_file

class Serializer:
    def serialize(self, topic_str, message_dict_list, message_separator_bytes):
        if is_file(topic_str):
            rel_file_str = topic_str
            #
            suffix_str = rel_file_str.split(".")[-1]
            #
            if suffix_str == "txt":
                return self.serialize_lines(message_dict_list, message_separator_bytes)
            elif suffix_str == "parquet":
                return self.serialize_parquet(message_dict_list, message_separator_bytes)
            else:
                raise Exception("Only \"txt\" supported.")
        else:
            return self.serialize_partition_bytes(message_dict_list, message_separator_bytes)

    def serialize_lines(self, message_dict_list, message_separator_bytes):
        messages_bytes = b""
        for message_dict in message_dict_list:
            message_bytes = str(message_dict["value"]).encode("utf-8") + message_separator_bytes
            #
            messages_bytes += message_bytes
        #
        return messages_bytes

    def serialize_parquet(self, message_dict_list, _):
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        #
        df = pd.DataFrame.from_records(value_dict_list)
        parquet_bytes = df.to_parquet()
        #
        return parquet_bytes

    def serialize_partition_bytes(self, message_dict_list, message_separator_bytes):
        messages_bytes = b""
        for message_dict in message_dict_list:
            message_dict["key"] = serialize_payload(message_dict["key"], self.key_type_str)
            message_dict["value"] = serialize_payload(message_dict["value"], self.value_type_str)
            message_bytes = str(message_dict).encode("utf-8") + message_separator_bytes
            #
            messages_bytes += message_bytes
        #
        return messages_bytes

#

def serialize_payload(payload, type_str):
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
