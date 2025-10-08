import os
import importlib
import json
import sys
import tempfile

from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from google.protobuf.json_format import MessageToDict

from kafi.schemaregistry import SchemaRegistry

class Deserializer(SchemaRegistry):
    def __init__(self, schema_registry_config_dict, **kwargs):
        self.deser_from_dict = kwargs["deser_from_dict"] if "deser_from_dict" in kwargs else None
        self.deser_conf = kwargs["deser_conf"] if "deser_conf" in kwargs else None
        self.deser_rule_conf = kwargs["deser_rule_conf"] if "deser_rule_conf" in kwargs else None
        self.deser_rule_registry = kwargs["deser_rule_registry"] if "deser_rule_registry" in kwargs else None
        self.deser_json_decode = kwargs["deser_json_decode"] if "deser_json_decode" in kwargs else None
        self.deser_return_record_name = kwargs["deser_return_record_name"] if "deser_return_record_name" in kwargs else False
        #
        super().__init__(schema_registry_config_dict)

    def deserialize(self, payload_bytes, type_str, topic_str, key_bool):
        if type_str.lower() == "bytes":
            deserialized_payload = self.bytes_to_bytes(payload_bytes)
        elif type_str.lower() in ["str", "string"]:
            deserialized_payload = self.bytes_to_str(payload_bytes)
        elif type_str.lower() == "json":
            deserialized_payload = self.bytes_to_dict(payload_bytes)
        elif type_str.lower() == "avro":
            deserialized_payload = self.bytes_avro_to_dict(payload_bytes, topic_str, key_bool)
        elif type_str.lower() in ["jsonschema", "json_sr"]:
            deserialized_payload = self.bytes_jsonschema_to_dict(payload_bytes, topic_str, key_bool)
        elif type_str.lower() in ["protobuf", "pb"]:
            deserialized_payload = self.bytes_protobuf_to_dict(payload_bytes, topic_str, key_bool)
        else:
            raise Exception("Only \"str\", \"bytes\", \"json\", \"protobuf\" (\"pb\"), \"avro\" and \"jsonschema\" (\"json_sr\") supported.")
        #
        return deserialized_payload

    def bytes_to_str(self, bytes):
        if bytes:
            return bytes.decode("utf-8")
        else:
            return bytes

    def bytes_to_bytes(self, bytes):
        return bytes

    def bytes_to_dict(self, bytes):
        if bytes is None:
            return None
        #
        return json.loads(bytes)

    def bytes_avro_to_dict(self, bytes, topic_str, key_bool):
        if bytes is None:
            return None
        #
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        schema_dict = self.get_schema(schema_id_int)
        schema_str = schema_dict["schema_str"]
        #
        avroDeserializer = AvroDeserializer(self.schemaRegistryClient, schema_str, self.deser_from_dict, self.deser_return_record_name, self.deser_conf, self.deser_rule_conf, self.deser_rule_registry)
        serializationContext = SerializationContext(topic_str, MessageField.KEY if key_bool else MessageField.VALUE)
        dict = avroDeserializer(bytes, serializationContext)
        return dict

    def bytes_jsonschema_to_dict(self, bytes, topic_str, key_bool):
        if bytes is None:
            return None
        #
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        schema_dict = self.get_schema(schema_id_int)
        schema_str = schema_dict["schema_str"]
        #
        jsonDeserializer = JSONDeserializer(schema_str, self.deser_from_dict, None, self.deser_conf, self.deser_rule_conf, self.deser_rule_registry, self.deser_json_decode)
        serializationContext = SerializationContext(topic_str, MessageField.KEY if key_bool else MessageField.VALUE)
        dict = jsonDeserializer(bytes, serializationContext)
        return dict

    def bytes_protobuf_to_dict(self, bytes, topic_str, key_bool):
        if bytes is None:
            return None
        #
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        if schema_id_int in self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict:
            generalizedProtocolMessageType, protobuf_schema_str = self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict[schema_id_int]
        else:
            generalizedProtocolMessageType, protobuf_schema_str = self.schema_id_int_to_generalizedProtocolMessageType_protobuf_schema_str_tuple(schema_id_int)
            self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict[schema_id_int] = (generalizedProtocolMessageType, protobuf_schema_str)
        #
        # Prevent: RuntimeError: ProtobufSerializer: the 'use.deprecated.format' configuration property must be explicitly set due to backward incompatibility with older confluent-kafka-python Protobuf producers and consumers. See the release notes for more details
        if self.deser_conf is None:
            self.deser_conf = {"use.deprecated.format": False}
        protobufDeserializer = ProtobufDeserializer(generalizedProtocolMessageType, self.deser_conf, None, self.deser_rule_conf, self.deser_rule_registry)
        serializationContext = SerializationContext(topic_str, MessageField.KEY if key_bool else MessageField.VALUE)
        protobuf_message = protobufDeserializer(bytes, serializationContext)
        dict = MessageToDict(protobuf_message)
        return dict

    # Helpers

    def schema_id_int_to_generalizedProtocolMessageType_protobuf_schema_str_tuple(self, schema_id_int):
        schema_dict = self.get_schema(schema_id_int)
        schema_str = schema_dict["schema_str"]
        #
        generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_id_int, schema_str)
        #
        return generalizedProtocolMessageType, schema_str

    def schema_id_int_and_schema_str_to_generalizedProtocolMessageType(self, schema_id_int, schema_str):
        path_str = f"/{tempfile.gettempdir()}/kafi/protobuf/{self.storage_obj.config_str}"
        os.makedirs(path_str, exist_ok=True)
        file_str = f"schema_{schema_id_int}.proto"
        file_path_str = f"{path_str}/{file_str}"
        with open(file_path_str, "w") as textIOWrapper:
            textIOWrapper.write(schema_str)
        #
        import grpc_tools.protoc
        grpc_tools.protoc.main(["protoc", f"-I{path_str}", f"--python_out={path_str}", f"{file_str}"])
        #
        sys.path.insert(1, path_str)
        schema_module = importlib.import_module(f"schema_{schema_id_int}_pb2")
        schema_name_str = list(schema_module.DESCRIPTOR.message_types_by_name.keys())[0]
        generalizedProtocolMessageType = getattr(schema_module, schema_name_str)
        return generalizedProtocolMessageType
