import os
import importlib
import json
import sys
import tempfile

from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from google.protobuf.json_format import MessageToDict
from kafi.schemaregistry import SchemaRegistry

class Deserializer:
    def deserialize(self, payload_bytes, type_str):
        if type_str.lower() == "bytes":
            deserialized_payload = self.bytes_to_bytes(payload_bytes)
        elif type_str.lower() == "str":
            deserialized_payload = self.bytes_to_str(payload_bytes)
        elif type_str.lower() == "json":
            deserialized_payload = self.bytes_to_dict(payload_bytes)
        elif type_str.lower() in ["protobuf", "pb"]:
            deserialized_payload = self.bytes_protobuf_to_dict(payload_bytes)
        elif type_str.lower() == "avro":
            deserialized_payload = self.bytes_avro_to_dict(payload_bytes)
        elif type_str.lower() in ["jsonschema", "json_sr"]:
            deserialized_payload = self.bytes_jsonschema_to_dict(payload_bytes)
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

    def bytes_protobuf_to_dict(self, bytes):
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
        protobufDeserializer = ProtobufDeserializer(generalizedProtocolMessageType, {"use.deprecated.format": False})
        protobuf_message = protobufDeserializer(bytes, None)
        dict = MessageToDict(protobuf_message)
        return dict

    def bytes_avro_to_dict(self, bytes):
        if bytes is None:
            return None
        #
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        schema_dict = self.schemaRegistry.get_schema(schema_id_int)
        schema_str = schema_dict["schema_str"]
        #
        avroDeserializer = AvroDeserializer(self.schemaRegistry.schemaRegistryClient, schema_str)
        dict = avroDeserializer(bytes, None)
        return dict

    def bytes_jsonschema_to_dict(self, bytes):
        if bytes is None:
            return None
        #
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        schema_dict = self.schemaRegistry.get_schema(schema_id_int)
        schema_str = schema_dict["schema_str"]
        #
        jsonDeserializer = JSONDeserializer(schema_str)
        dict = jsonDeserializer(bytes, None)
        return dict

    # Helpers

    def schema_id_int_to_generalizedProtocolMessageType_protobuf_schema_str_tuple(self, schema_id_int):
        schema_dict = self.schemaRegistry.get_schema(schema_id_int)
        schema_str = schema_dict["schema_str"]
        #
        generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_id_int, schema_str)
        #
        return generalizedProtocolMessageType, schema_str

    def schema_id_int_and_schema_str_to_generalizedProtocolMessageType(self, schema_id_int, schema_str):
        path_str = f"/{tempfile.gettempdir()}/kafi/clusters/{self.storage_obj.config_str}"
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
