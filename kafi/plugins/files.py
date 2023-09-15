import json
import os

# Constants

ALL_MESSAGES = -1

#

class Files():
    def to_file(self, topic, fs_obj, file, n=ALL_MESSAGES, **kwargs):
        file_str = file
        #
        def foldl_function(acc, message_dict):
            value_bytes = json.dumps(message_dict["value"]).encode("utf-8")
            #
            if key_value_separator_bytes == None:
                line_bytes = value_bytes
            else:
                key_bytes = json.dumps(message_dict["key"]).encode("utf-8")
                line_bytes = key_bytes + key_value_separator_bytes + value_bytes
            #
            line_bytes += message_separator_bytes
            #
            acc += line_bytes
            #
            return acc
        #
        
        message_separator_bytes = kwargs["message_separator"] if "message_separator" in kwargs else b"\n"
        key_value_separator_bytes = kwargs["key_value_separator"] if "key_value_separator" in kwargs else None
        #
        abs_path_file_str = os.path.join(fs_obj.root_dir(), "files", file_str)
        #
        (lines_bytes, n_int) = self.foldl(topic, foldl_function, b"", n, **kwargs)
        #
        fs_obj.admin.write_bytes(abs_path_file_str, lines_bytes)
        #
        return n_int

    def from_file(self, fs_obj, file, topic, n=ALL_MESSAGES, **kwargs):
        file_str = file
        topic_str = topic
        n_int = n
        #
        message_separator_bytes = kwargs["message_separator"] if "message_separator" in kwargs else b"\n"
        key_value_separator_bytes = kwargs["key_value_separator"] if "key_value_separator" in kwargs else None
        #
        def split_line(line_bytes):
            key_bytes = None
            value_bytes = None
            #
            if line_bytes:
                if key_value_separator_bytes is not None:
                    split_bytes_list = line_bytes.split(key_value_separator_bytes)
                    if len(split_bytes_list) == 2:
                        key_bytes = split_bytes_list[0]
                        value_bytes = split_bytes_list[1]
                    else:
                        value_bytes = line_bytes
                else:
                    value_bytes = line_bytes
            #
            return key_bytes, value_bytes 
        #

        abs_path_file_str = os.path.join(fs_obj.root_dir(), "files", file_str)
        #
        lines_bytes = fs_obj.admin.read_bytes(abs_path_file_str)
        #
        line_bytes_list = lines_bytes.split(message_separator_bytes)[:-1]
        #
        producer = self.producer(topic_str, **kwargs)
        line_counter_int = 0
        for line_bytes in line_bytes_list:
            if n_int != ALL_MESSAGES:
                if line_counter_int >= n_int:
                    break
            #
            (key_bytes, value_bytes) = split_line(line_bytes)
            #
            producer.produce(value_bytes, key=key_bytes)
            #
            line_counter_int += 1
        producer.close()
        #
        return line_counter_int
