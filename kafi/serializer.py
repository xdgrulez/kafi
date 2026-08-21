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

from kafi.schemaregistry import SchemaRegistry
from kafi.helpers import to_bytes

class Serializer(SchemaRegistry):
    def __init__(self, schema_registry_config_dict, **kwargs):
        """Build key/value serializers for a topic from schema registry config and kwargs.

        Args:
            schema_registry_config_dict: schema.registry.url etc.
            **kwargs: key_type/value_type-driving config, e.g. key_schema, value_schema, ser_conf, ..."""
        super().__init__(schema_registry_config_dict)
        #
        self.get_configs_from_kwargs(**kwargs)
        #
        self.key_serializer = self.get_serializer(True)
        self.value_serializer = self.get_serializer(False)


    def serialize(self, payload, key_bool):
        """Serialize a key or value payload according to its configured type (bytes/str/json/avro/jsonschema/protobuf).

        Args:
            payload: value to serialize (bytes, str, dict, or arbitrary)
            key_bool: True to serialize as the key, False as the value"""
        type_str = self.key_type_str if key_bool else self.value_type_str
        messageField = MessageField.KEY if key_bool else MessageField.VALUE
        serializer = self.key_serializer if key_bool else self.value_serializer
        #
        def payload_to_serializer_payload():
            try:
                if isinstance(payload, bytes):
                    serializer_payload = json.loads(payload)
                elif isinstance(payload, str):
                    serializer_payload = json.loads(payload)
                else:
                    serializer_payload = payload
            except (json.JSONDecodeError, TypeError):
                serializer_payload = payload
            #
            return serializer_payload
        #
        if payload == None:
            serialized_payload_bytes = None
        else:
            if type_str.lower() in ["bytes", "str", "string", "json"]:
                serialized_payload_bytes = to_bytes(payload)
            elif type_str.lower() == "avro":
                payload_dict = payload_to_serializer_payload()
                serialized_payload_bytes = serializer(payload_dict, SerializationContext(self.topic_str, messageField))
            elif type_str.lower() in ["jsonschema", "json_sr"]:
                payload_dict = payload_to_serializer_payload()
                serialized_payload_bytes = serializer(payload_dict, SerializationContext(self.topic_str, messageField))
            elif type_str.lower() in ["pb", "protobuf"]:
                generalizedProtocolMessageType = self.key_generalizedProtocolMessageType if key_bool else self.value_generalizedProtocolMessageType
                payload_dict = payload_to_serializer_payload()
                protobuf_message = generalizedProtocolMessageType()
                ParseDict(payload_dict, protobuf_message)
                serialized_payload_bytes = serializer(protobuf_message, SerializationContext(self.topic_str, messageField))
            else:
                raise Exception("Only \"bytes\", \"str\", \"json\", \"avro\", \"protobuf\" (\"pb\") and \"jsonschema\" (\"json_sr\") supported.")
        #
        return serialized_payload_bytes

    # Helpers

    def schema_str_to_generalizedProtocolMessageType(self, schema_str, topic_str, key_bool, normalize_schemas=False):
        """Get (registering if needed) the generated Python protobuf message class for a schema.

        Args:
            schema_str: .proto schema source
            topic_str: topic the schema is registered under
            key_bool: True for the key schema, False for the value schema
            normalize_schemas: whether to normalize the schema before registering"""
        schema_hash_int = hash(schema_str)
        if schema_hash_int in self.schema_hash_int_generalizedProtocolMessageType_dict:
            generalizedProtocolMessageType = self.schema_hash_int_generalizedProtocolMessageType_dict[schema_hash_int]
        else:
            subject_name_str = self.create_subject_name_str(topic_str, key_bool)
            schema_dict = self.create_schema_dict(schema_str, "PROTOBUF")
            schema_id_int = self.register_schema(subject_name_str, schema_dict, normalize_schemas)
            #
            generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_id_int, schema_str)
            #
            self.schema_hash_int_generalizedProtocolMessageType_dict[schema_hash_int] = generalizedProtocolMessageType
        #
        return generalizedProtocolMessageType

    def schema_id_int_and_schema_str_to_generalizedProtocolMessageType(self, schema_id_int, schema_str):
        """Compile a .proto schema to a Python protobuf message class via grpc_tools.protoc.

        Args:
            schema_id_int: schema registry id, used to name the generated module
            schema_str: .proto schema source"""
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

    #

    def get_serializer(self, key_bool):
        """Build the confluent_kafka serializer instance for the configured type (or None for bytes/str/json).

        Args:
            key_bool: True to build the key serializer, False for the value serializer"""
        type_str = self.key_type_str if key_bool else self.value_type_str
        schema_str_or_dict = self.key_schema_str_or_dict if key_bool else self.value_schema_str_or_dict
        schema_id_int = self.key_schema_id_int if key_bool else self.value_schema_id_int
        #
        ser_to_dict = self.key_ser_to_dict if key_bool else self.value_ser_to_dict
        ser_conf = self.key_ser_conf if key_bool else self.value_ser_conf
        ser_rule_conf = self.key_ser_rule_conf if key_bool else self.value_ser_rule_conf
        ser_rule_registry = self.key_ser_rule_registry if key_bool else self.value_ser_rule_registry
        ser_json_encode = self.key_ser_json_encode if key_bool else self.value_ser_json_encode
        normalize_schemas = self.key_normalize_schemas if key_bool else self.value_normalize_schemas
        #
        def get_schema_str():
            if schema_str_or_dict is None:
                if schema_id_int is None:
                    raise Exception("Please provide a schema or schema ID for the " + ("key" if key_bool else "value") + ".")
                schema_str = self.schemaRegistryClient.get_schema(schema_id_int)
            else:
                if isinstance(schema_str_or_dict, str):
                    schema_str = schema_str_or_dict
                elif isinstance(schema_str_or_dict, dict):
                    schema_str = json.dumps(schema_str_or_dict)
            #
            return schema_str
        #
        if type_str.lower() in ["bytes", "str", "string", "json"]:
            return None
        elif type_str.lower() == "avro":
            schema = get_schema_str()
            #
            return AvroSerializer(self.schemaRegistryClient, schema, ser_to_dict, ser_conf, ser_rule_conf, ser_rule_registry)
        elif type_str.lower() in ["jsonschema", "json_sr"]:
            schema = get_schema_str()
            #
            return JSONSerializer(schema, self.schemaRegistryClient, ser_to_dict, ser_conf, ser_rule_conf, ser_rule_registry, ser_json_encode)
        elif type_str.lower() in ["pb", "protobuf"]:
            schema = get_schema_str()
            generalizedProtocolMessageType = self.schema_str_to_generalizedProtocolMessageType(schema, self.topic_str, key_bool, normalize_schemas)
            #
            if key_bool:
                self.key_generalizedProtocolMessageType = generalizedProtocolMessageType
            else:
                self.value_generalizedProtocolMessageType = generalizedProtocolMessageType
            #
            # Prevent: RuntimeError: ProtobufSerializer: the 'use.deprecated.format' configuration property must be explicitly set due to backward incompatibility with older confluent-kafka-python Protobuf producers and consumers. See the release notes for more details
            if ser_conf is None:
                ser_conf = {"use.deprecated.format": False}
            #
            return ProtobufSerializer(generalizedProtocolMessageType, self.schemaRegistryClient, ser_conf, ser_rule_conf, ser_rule_registry)
        else:
            raise Exception("Only \"bytes\", \"str\", \"json\", \"avro\", \"protobuf\" (\"pb\") and \"jsonschema\" (\"json_sr\") supported.")

    #

    def get_configs_from_kwargs(self, **kwargs):
        """Populate key/value ser_* settings from kwargs, applying key_/value_-prefixed overrides.

        Args:
            **kwargs: ser_to_dict, ser_conf, ser_rule_conf, ser_rule_registry, ser_json_encode, normalize_schemas (each optionally key_/value_-prefixed)"""
        self.key_ser_to_dict = None
        self.value_ser_to_dict = None
        if "ser_to_dict" in kwargs:
            self.key_ser_to_dict = kwargs["ser_to_dict"]
            self.value_ser_to_dict = kwargs["ser_to_dict"]
        if "key_ser_to_dict" in kwargs:
            self.key_ser_to_dict = kwargs["key_ser_to_dict"]
        if "value_ser_to_dict" in kwargs:
            self.value_ser_to_dict = kwargs["value_ser_to_dict"]
        #
        self.key_ser_conf = None
        self.value_ser_conf = None
        if "ser_conf" in kwargs:
            self.key_ser_conf = kwargs["ser_conf"]
            self.value_ser_conf = kwargs["ser_conf"]
        if "key_ser_conf" in kwargs:
            self.key_ser_conf = kwargs["key_ser_conf"]
        if "value_ser_conf" in kwargs:
            self.value_ser_conf = kwargs["value_ser_conf"]
        #
        self.key_ser_rule_conf = None
        self.value_ser_rule_conf = None
        if "ser_rule_conf" in kwargs:
            self.key_ser_rule_conf = kwargs["ser_rule_conf"]
            self.value_ser_rule_conf = kwargs["ser_rule_conf"]
        if "key_ser_rule_conf" in kwargs:
            self.key_ser_rule_conf = kwargs["key_ser_rule_conf"]
        if "value_ser_rule_conf" in kwargs:
            self.value_ser_rule_conf = kwargs["value_ser_rule_conf"]
        #
        self.key_ser_rule_registry = None
        self.value_ser_rule_registry = None
        if "ser_rule_registry" in kwargs:
            self.key_ser_rule_registry = kwargs["ser_rule_registry"]
            self.value_ser_rule_registry = kwargs["ser_rule_registry"]
        if "key_ser_rule_registry" in kwargs:
            self.key_ser_rule_registry = kwargs["key_ser_rule_registry"]
        if "value_ser_rule_registry" in kwargs:
            self.value_ser_rule_registry = kwargs["value_ser_rule_registry"]
        #
        self.key_ser_json_encode = None
        self.value_ser_json_encode = None
        if "ser_json_encode" in kwargs:
            self.key_ser_json_encode = kwargs["ser_json_encode"]
            self.value_ser_json_encode = kwargs["ser_json_encode"]
        if "key_ser_json_encode" in kwargs:
            self.key_ser_json_encode = kwargs["key_ser_json_encode"]
        if "value_ser_json_encode" in kwargs:
            self.value_ser_json_encode = kwargs["value_ser_json_encode"]
        #
        self.key_normalize_schemas = False
        self.value_normalize_schemas = False
        if "normalize_schemas" in kwargs:
            self.key_normalize_schemas = kwargs["normalize_schemas"]
            self.value_normalize_schemas = kwargs["normalize_schemas"]
        if "key_normalize_schemas" in kwargs:
            self.key_normalize_schemas = kwargs["key_normalize_schemas"]
        if "value_normalize_schemas" in kwargs:
            self.value_normalize_schemas = kwargs["value_normalize_schemas"]
