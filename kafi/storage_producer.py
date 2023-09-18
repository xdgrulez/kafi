from kafi.serializer import Serializer
from kafi.schemaregistry import SchemaRegistry

class StorageProducer(Serializer):
    def __init__(self, storage_obj, topic, **kwargs):
        self.storage_obj = storage_obj
        #
        self.topic_str = topic
        #
        (self.key_type_str, self.value_type_str) = storage_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.key_schema_str, self.value_schema_str, self.key_schema_id_int, self.value_schema_id_int) = self.get_key_value_schema_tuple(**kwargs)
        #
        self.keep_partitions_bool = kwargs["keep_partitions"] if "keep_partitions" in kwargs else False
        #
        self.keep_timestamps_bool = kwargs["keep_timestamps"] if "keep_timestamps" in kwargs else False
        #
        self.written_counter_int = 0
        #
        self.schema_hash_int_generalizedProtocolMessageType_dict = {}
        #
        if "schema.registry.url" in self.storage_obj.schema_registry_config_dict:
            self.schemaRegistry = SchemaRegistry(self.storage_obj.schema_registry_config_dict, self.storage_obj.kafi_config_dict)
        else:
            self.schemaRegistry = None

    #

    def produce_list(self, message_dict_list, **kwargs):
        value_list = [message_dict["value"] for message_dict in message_dict_list]
        #
        key_list = [message_dict["key"] for message_dict in message_dict_list]
        #
        if self.keep_partitions_bool:
            partition_list = [message_dict["partition"] for message_dict in message_dict_list]
        else:
            partition_list = None
        #
        if self.keep_timestamps_bool and message_dict_list[0]["timestamp"] is not None:
            timestamp_list = [message_dict["timestamp"] for message_dict in message_dict_list]
        else:
            timestamp_list = None
        #
        headers_list = [message_dict["headers"] for message_dict in message_dict_list]
        #
        return self.produce(value_list, key=key_list, partition=partition_list, timestamp=timestamp_list, headers=headers_list, **kwargs)

    # Helpers

    def get_key_value_schema_tuple(self, **kwargs):
        key_schema_str = kwargs["key_schema"] if "key_schema" in kwargs else None
        value_schema_str = kwargs["value_schema"] if "value_schema" in kwargs else None
        #
        key_schema_id_int = kwargs["key_schema_id"] if "key_schema_id" in kwargs else None
        value_schema_id_int = kwargs["value_schema_id"] if "value_schema_id" in kwargs else None
        #
        return (key_schema_str, value_schema_str, key_schema_id_int, value_schema_id_int)
