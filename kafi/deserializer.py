import ast
import json

class Deserializer:
    def deserialize(self, topic_str, message_bytes, message_separator_bytes, key_type_str, value_type_str):
        if topic_str.startswith("file,"):
            rel_file_str = topic_str.split(",")[1]
            #
            suffix_str = rel_file_str.split(".")[-1]
            #
            if suffix_str == "txt":
                return self.deserialize_lines(message_bytes, message_separator_bytes, key_type_str, value_type_str)
            else:
                raise Exception("Only \"txt\" supported.")
        else:
            return self.deserialize_partition_bytes(message_bytes, message_separator_bytes, key_type_str, value_type_str)
    
    #

    def deserialize_partition_bytes(self, message_bytes, message_separator_bytes, key_type_str, value_type_str):
        def to_str(x):
            if isinstance(x, bytes):
                return x.decode("utf-8")
            elif isinstance(x, dict):
                return str(x)
            else:
                return x
        #

        def to_bytes(x):
            if isinstance(x, str):
                return x.encode("utf-8")
            elif isinstance(x, dict):
                return str(x).encode("utf-8")
            else:
                return x
        #

        def to_dict(x):
            if isinstance(x, bytes) or isinstance(x, str):
                return json.loads(x)
            else:
                return x
        #

        if key_type_str.lower() == "str":
            decode_key = to_str
        elif key_type_str.lower() == "bytes":
            decode_key = to_bytes
        elif key_type_str.lower() == "json":
            decode_key = to_dict
        else:
            raise Exception("Only json, str or bytes supported.")
        #
        if value_type_str.lower() == "str":
            decode_value = to_str
        elif value_type_str.lower() == "bytes":
            decode_value = to_bytes
        elif value_type_str.lower() == "json":
            decode_value = to_dict
        else:
            raise Exception("Only json, str or bytes supported.")
        #

        message_bytes_list = message_bytes.split(message_separator_bytes)[:-1]
        #
        message_dict_list = []
        for message_bytes in message_bytes_list:
            parsed_message_dict = ast.literal_eval(message_bytes.decode("utf-8"))
            #
            message_dict = {"headers": parsed_message_dict["headers"], "timestamp": parsed_message_dict["timestamp"], "key": decode_key(parsed_message_dict["key"]), "value": decode_value(parsed_message_dict["value"]), "offset": parsed_message_dict["offset"], "partition": parsed_message_dict["partition"]}
            message_dict_list.append(message_dict)
        #
        return message_dict_list

    def deserialize_lines(self, message_bytes, message_separator_bytes, key_type_str, value_type_str):
        def to_str(x):
            if isinstance(x, bytes):
                return x.decode("utf-8")
            elif isinstance(x, dict):
                return str(x)
            else:
                return x
        #

        def to_bytes(x):
            if isinstance(x, str):
                return x.encode("utf-8")
            elif isinstance(x, dict):
                return str(x).encode("utf-8")
            else:
                return x
        #

        def to_dict(x):
            if isinstance(x, bytes) or isinstance(x, str):
                return json.loads(x)
            else:
                return x
        #

        if key_type_str.lower() == "str":
            decode_key = to_str
        elif key_type_str.lower() == "bytes":
            decode_key = to_bytes
        elif key_type_str.lower() == "json":
            decode_key = to_dict
        else:
            raise Exception("Only json, str or bytes supported.")
        #
        if value_type_str.lower() == "str":
            decode_value = to_str
        elif value_type_str.lower() == "bytes":
            decode_value = to_bytes
        elif value_type_str.lower() == "json":
            decode_value = to_dict
        else:
            raise Exception("Only json, str or bytes supported.")
        #

        message_bytes_list = message_bytes.split(message_separator_bytes)[:-1]
        #
        message_dict_list = []
        offset_counter_int = 0
        for message_bytes in message_bytes_list:            
            message_dict = {"headers": None, "timestamp": None, "key": None, "value": decode_value(message_bytes), "offset": offset_counter_int, "partition": 0}
            #
            offset_counter_int += 1
            #
            message_dict_list.append(message_dict)
        #
        return message_dict_list
