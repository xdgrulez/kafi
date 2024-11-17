import importlib
import json
import os
import sys
import tempfile

from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from google.protobuf.json_format import ParseDict

from kafi.helpers import to_bytes

class Serializer:
    def serialize(self, payload, key_bool, normalize_schemas=False):
        type_str = self.key_type_str if key_bool else self.value_type_str
        schema_str = self.key_schema_str if key_bool else self.value_schema_str
        schema_id_int = self.key_schema_id_int if key_bool else self.value_schema_id_int
        messageField = MessageField.KEY if key_bool else MessageField.VALUE
        #
        def get_schema():
            if schema_str is None:
                if schema_id_int is None:
                    raise Exception("Please provide a schema or schema ID for the " + ("key" if key_bool else "value") + ".")
                schema = self.sr.schemaRegistryClient.get_schema(schema_id_int)
            else:
                schema = schema_str
            #
            return schema
        #

        def payload_to_payload_dict():
            if isinstance(payload, bytes):
                payload_dict = json.loads(payload)
            elif isinstance(payload, str):
                payload_dict = json.loads(payload)
            elif isinstance(payload, dict):
                payload_dict = payload
            #
            return payload_dict
        #
        if payload == None:
            serialized_payload_bytes = None
        else:
            if type_str.lower() in ["bytes", "str", "json"]:
                serialized_payload_bytes = to_bytes(payload)
            elif type_str.lower() in ["pb", "protobuf"]:
                schema = get_schema()
                generalizedProtocolMessageType = self.schema_str_to_generalizedProtocolMessageType(schema, self.topic_str, key_bool, normalize_schemas)
                protobufSerializer = ProtobufSerializer(generalizedProtocolMessageType, self.sr.schemaRegistryClient, {"use.deprecated.format": False})
                payload_dict = payload_to_payload_dict()
                protobuf_message = generalizedProtocolMessageType()
                ParseDict(payload_dict, protobuf_message)
                serialized_payload_bytes = protobufSerializer(protobuf_message, SerializationContext(self.topic_str, messageField))
            elif type_str.lower() == "avro":
                schema = get_schema()
                avroSerializer = AvroSerializer(self.sr.schemaRegistryClient, schema)
                payload_dict = payload_to_payload_dict()
                serialized_payload_bytes = avroSerializer(payload_dict, SerializationContext(self.topic_str, messageField))
            elif type_str.lower() in ["jsonschema", "json_sr"]:
                payload_dict = payload_to_payload_dict()
                schema = get_schema()
                jSONSerializer = JSONSerializer(schema, self.sr.schemaRegistryClient)
                serialized_payload_bytes = jSONSerializer(payload_dict, SerializationContext(self.topic_str, messageField))
            else:
                raise Exception("Only \"bytes\", \"str\", \"json\", \"avro\", \"protobuf\" (\"pb\") and \"jsonschema\" (\"json_sr\") supported.")
        #
        return serialized_payload_bytes

    # Helpers

    def schema_str_to_generalizedProtocolMessageType(self, schema_str, topic_str, key_bool, normalize_schemas=False):
        schema_hash_int = hash(schema_str)
        if schema_hash_int in self.schema_hash_int_generalizedProtocolMessageType_dict:
            generalizedProtocolMessageType = self.schema_hash_int_generalizedProtocolMessageType_dict[schema_hash_int]
        else:
            subject_name_str = self.sr.create_subject_name_str(topic_str, key_bool)
            schema_dict = self.sr.create_schema_dict(schema_str, "PROTOBUF")
            schema_id_int = self.sr.register_schema(subject_name_str, schema_dict, normalize_schemas)
            #
            generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_id_int, schema_str)
            #
            self.schema_hash_int_generalizedProtocolMessageType_dict[schema_hash_int] = generalizedProtocolMessageType
        #
        return generalizedProtocolMessageType

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
