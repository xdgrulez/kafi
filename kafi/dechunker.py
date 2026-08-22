from kafi.deserializer import Deserializer
from kafi.helpers import chunk_key_to_key

#

class Dechunker(Deserializer):
    def __init__(self, schema_registry_config_dict, **kwargs):
        """Initialize the in-progress chunk-reassembly buffer.

        Args:
            schema_registry_config_dict: schema.registry.url etc., passed through to Deserializer
            **kwargs: passed through to Deserializer"""
        # Dictionary mapping chunked message IDs to chunk numbers to chunk value_bytes (to reconstruct chunked messages).
        self.chunks_dict = {}
        #
        super().__init__(schema_registry_config_dict, **kwargs)

    #

    def dechunk(self, m_list):
        """Reassemble chunked messages, buffering partial chunks until complete; passes non-chunked messages through.

        Args:
            m_list: messages to dechunk (some may carry chunking headers, most won't)

        Returns:
            list of m: non-chunked messages plus any messages that were fully reassembled"""
        m_list1 = []
        for m in m_list:
            topic_str = m["topic"]
            # Get dictionary of headers
            headers_str_bytes_tuple_list = m["headers"]
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
                self.chunks_dict[chunked_message_id_str][chunk_number_int] = m["value"]
                #
                if all(value_bytes is not None for value_bytes in self.chunks_dict[chunked_message_id_str].values()):
                    dechunked_value_bytes = b""
                    #
                    for chunk_number_int1, value_bytes in self.chunks_dict[chunked_message_id_str].items():
                        # Special handling if the values were serialized in conjunction with Schema Registry.
                        if self.topic_str_value_type_str_dict[topic_str] in ["avro", "jsonschema", "json_sr", "pb", "protobuf"]:
                            # If so, skip the first five bytes from all but the first chunk (upon produce, we add the first five bytes to all messages to avoid confluent.value.schema.validation == true blocking them).
                            if chunk_number_int1 == 0:
                                dechunked_value_bytes += value_bytes
                            else:
                                dechunked_value_bytes += value_bytes[5:]
                        # Else just dechunk.
                        else:
                            dechunked_value_bytes += value_bytes
                    #
                    key_bytes = chunk_key_to_key(m["key"])
                    #
                    # Delete the header fields for chunking.
                    del headers_str_bytes_dict["kafi_chunked_message_id"]
                    del headers_str_bytes_dict["kafi_number_of_chunks"]
                    del headers_str_bytes_dict["kafi_chunk_number"]
                    headers_str_bytes_tuple_list = list(headers_str_bytes_dict.items())
                    #
                    m2 = {"value": dechunked_value_bytes,
                                     "key": key_bytes,
                                     "headers": headers_str_bytes_tuple_list,
                                     "timestamp": m["timestamp"],
                                     "partition": m["partition"],
                                     "offset": m["offset"],
                                     "topic": m["topic"]}
                    m_list1.append(m2)
                    #
                    # Clean up the chunks dictionary.
                    del self.chunks_dict[chunked_message_id_str]
            else:
                m_list1.append(m)
        #
        return m_list1
