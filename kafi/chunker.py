import uuid

from kafi.serializer import Serializer
from kafi.helpers import m_chunk_key_to_key, default_partitioner, key_to_chunk_key, split_bytes

#

class Chunker(Serializer):
    def __init__(self, schema_registry_config_dict, **kwargs):
        """Configure chunking (splitting oversized values across multiple messages) from kwargs.

        Args:
            schema_registry_config_dict: schema.registry.url etc., passed through to Serializer
            **kwargs: "chunk_size_bytes" enables chunking above that size; -1 (default) disables it"""
        self.chunk_size_bytes_int = kwargs["chunk_size_bytes"] if "chunk_size_bytes" in kwargs else -1
        if self.chunk_size_bytes_int == 0:
            raise Exception("Chunk size is zero.")
        if self.chunk_size_bytes_int > 0 and self.__class__.__name__ == "RestProxyProducer":
            raise Exception("Chunking not supported for RestProxy storage.")
        #
        if self.chunk_size_bytes_int > 0:
            self.partitioner_fun = default_partitioner
            self.projection_fun = m_chunk_key_to_key
        #
        super().__init__(schema_registry_config_dict, **kwargs)


    #

    def chunk(self, m_list):
        """Split any oversized message value into multiple chunk messages, tagged with chunking headers.

        Args:
            m_list: messages to chunk (already serialized to bytes)"""
        m_list1 = []
        if self.chunk_size_bytes_int > 0:
            for m in m_list:
                value_bytes = m["value"]
                #
                if len(value_bytes) > self.chunk_size_bytes_int:
                    chunk_value_bytes_list = split_bytes(value_bytes, self.chunk_size_bytes_int)
                    #
                    headers_str_bytes_tuple_list = m["headers"]
                    if headers_str_bytes_tuple_list is None:
                        headers_str_bytes_tuple_list = []
                    headers_str_bytes_dict = dict(headers_str_bytes_tuple_list)
                    headers_str_bytes_dict["kafi_chunked_message_id"] = bytes(str(uuid.uuid4()), "UTF-8")
                    headers_str_bytes_dict["kafi_number_of_chunks"] = len(chunk_value_bytes_list).to_bytes(32, byteorder="big")
                    #
                    for chunk_int, chunk_value_bytes in zip(range(len(chunk_value_bytes_list)), chunk_value_bytes_list):
                        # If the first byte of the value starts with 0 we assume this is a message serialized using Schema Registry. In that case, add the five bytes from the beginning of the message to each chunk (to avoid confluent.value.schema.validation == true blocking the individual chunks).
                        if self.value_type_str in ["avro", "jsonschema", "json_sr", "pb", "protobuf"] and chunk_int > 0:
                            chunk_value_bytes = value_bytes[0:5] + chunk_value_bytes
                        #
                        chunk_key_bytes = key_to_chunk_key(m["key"], chunk_int)
                        #
                        headers_str_bytes_dict["kafi_chunk_number"] = chunk_int.to_bytes(32, byteorder="big")
                        #
                        chunk_headers_str_bytes_tuple_list = list(headers_str_bytes_dict.items())
                        #
                        m1 = {"value": chunk_value_bytes,
                                         "key": chunk_key_bytes,
                                         "partition": m["partition"],
                                         "timestamp": m["timestamp"],
                                         "headers": chunk_headers_str_bytes_tuple_list}
                        m_list1.append(m1)
                else:
                    m_list1 = m_list
        else:
            m_list1 = m_list
        #
        return m_list1
