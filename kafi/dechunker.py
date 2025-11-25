from kafi.deserializer import Deserializer
from kafi.helpers import chunk_key_to_key

#

class Dechunker(Deserializer):
    def __init__(self, schema_registry_config_dict, **kwargs):
        super().__init__(schema_registry_config_dict, **kwargs)
        #
        # Dictionary mapping chunked message IDs to chunk numbers to chunk value_bytes (to reconstruct chunked messages).
        self.chunks_dict = {}

    #

    def dechunk(self, message_dict_list):
        message_dict_list1 = []
        for message_dict in message_dict_list:
            # Get dictionary of headers
            headers_str_bytes_tuple_list = message_dict["headers"]
            if headers_str_bytes_tuple_list is None:
                headers_str_bytes_tuple_list = []
            headers_str_bytes_dict = dict(headers_str_bytes_tuple_list)
            #
            if "kafi_chunked_message_id" in headers_str_bytes_dict:
                #
                chunked_message_id_str = str(headers_str_bytes_dict["kafi_chunked_message_id"])
                #
                number_of_chunks_int = int.from_bytes(headers_str_bytes_dict["kafi_number_of_chunks"])
                #
                chunk_number_int = int.from_bytes(headers_str_bytes_dict["kafi_chunk_number"])
                #
                if chunked_message_id_str not in self.chunks_dict:
                    self.chunks_dict[chunked_message_id_str] = {chunk_number_int1: None for chunk_number_int1 in range(number_of_chunks_int)}
                #
                self.chunks_dict[chunked_message_id_str][chunk_number_int] = message_dict["value"]
                #
                if all(value_bytes is not None for value_bytes in self.chunks_dict[chunked_message_id_str].values()):
                    dechunked_value_bytes = b""
                    #
                    for chunk_number_int1, value_bytes in self.chunks_dict[chunked_message_id_str].items():
                        # Special handling if the values were serialized in conjunction with Schema Registry.
                        if value_bytes[0] == b"0":
                            # If so, skip the first five bytes from all but the first chunk (upon produce, we add the first five bytes to all messages to avoid confluent.value.schema.validation == true blocking them).
                            if chunk_number_int1 == 0:
                                dechunked_value_bytes += value_bytes
                            else:
                                dechunked_value_bytes += value_bytes[5:]
                        # Else just dechunk.
                        else:
                            dechunked_value_bytes += value_bytes
                    #
                    key_bytes = chunk_key_to_key(message_dict["key"])
                    #
                    # Delete the header fields for chunking.
                    del headers_str_bytes_dict["kafi_chunked_message_id"]
                    del headers_str_bytes_dict["kafi_number_of_chunks"]
                    del headers_str_bytes_dict["kafi_chunk_number"]
                    headers_str_bytes_tuple_list = list(headers_str_bytes_dict.items())
                    #
                    message_dict2 = {"value": dechunked_value_bytes,
                                        "key": key_bytes,
                                        "headers": headers_str_bytes_tuple_list,
                                        "timestamp": message_dict["timestamp"],
                                        "partition": message_dict["partition"],
                                        "offset": message_dict["offset"],
                                        "topic": message_dict["topic"]}
                    message_dict_list1.append(message_dict2)
                    #
                    # Clean up the chunks dictionary.
                    del self.chunks_dict[chunked_message_id_str]
            else:
                message_dict_list1.append(message_dict)
        #
        return message_dict_list1
