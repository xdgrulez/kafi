from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from kafi.helpers import pattern_match, get, delete

class SchemaRegistry:
    def __init__(self, schema_registry_config_dict):
        self.schema_registry_config_dict = schema_registry_config_dict
        #
        if self.schema_registry_config_dict == {}:
            self.schemaRegistryClient = None
        else:
            self.schemaRegistryClient = self.get_schemaRegistryClient()

    def get_schemaRegistryClient(self):
        dict = {}
        #
        dict["url"] = self.schema_registry_config_dict["schema.registry.url"]
        if "basic.auth.user.info" in self.schema_registry_config_dict:
            dict["basic.auth.user.info"] = self.schema_registry_config_dict["basic.auth.user.info"]
        #
        schemaRegistryClient = SchemaRegistryClient(dict)
        return schemaRegistryClient

    def get_schema(self, schema_id):
        # No additional caching necessary here:
        # get_schema(schema_id)[source]
        # Fetches the schema associated with schema_id from the Schema Registry. The result is cached so subsequent attempts will not require an additional round-trip to the Schema Registry.
        schema_id_int = schema_id
        #
        schema = self.schemaRegistryClient.get_schema(schema_id_int)
        schema_dict = schema_to_schema_dict(schema)
        #
        return schema_dict

    def create_schema_dict(self, schema_str, schema_type_str):
        schema = Schema(schema_str, schema_type_str) # TODO: support references
        schema_dict = schema_to_schema_dict(schema)
        #
        return schema_dict

    def create_subject_name_str(self, topic_str, key_bool):
        key_or_value_str = "key" if key_bool else "value"
        #
        subject_name_str = f"{topic_str}-{key_or_value_str}"
        #
        return subject_name_str

    def register_schema(self, subject_name, schema, normalize=False):
        subject_name_str = subject_name
        schema_dict = schema
        normalize_bool = normalize
        #
        schema1 = Schema(schema_dict["schema_str"], schema_dict["schema_type"]) # TODO: support references
        #
        schema_id_int = self.schemaRegistryClient.register_schema(subject_name_str, schema1, normalize_schemas=normalize_bool)
        return schema_id_int

    def lookup_schema(self, subject_name, schema, normalize=False):
        subject_name_str = subject_name
        schema_dict = schema
        normalize_schemas_bool = normalize
        #
        schema1 = Schema(schema_dict["schema_str"], schema_dict["schema_type"]) # TODO: support references
        #
        registeredSchema = self.schemaRegistryClient.lookup_schema(subject_name_str, schema1, normalize_schemas=normalize_schemas_bool)
        registeredSchema_dict = registeredSchema_to_registeredSchema_dict(registeredSchema)
        #
        return registeredSchema_dict

    def get_subjects(self, pattern=None, deleted=False):
        deleted_bool = deleted
        #
        if deleted_bool:
            url_str = f"{self.schema_registry_config_dict['schema.registry.url']}/subjects?deleted=true"
            headers_dict = {"Accept": "application/json"}
            auth_str_tuple = None
            #
            if "basic.auth.credentials.source" in self.schema_registry_config_dict and self.schema_registry_config_dict ["basic.auth.credentials.source"] == "USER_INFO":
                basic_auth_user_info_str = self.schema_registry_config_dict["basic.auth.user.info"]
                auth_str_tuple = tuple(basic_auth_user_info_str.split(":"))
            #
            subject_name_str_list = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, debug_bool=self.verbose() >= 2)
        else:
            subject_name_str_list = self.schemaRegistryClient.get_subjects()
        
        filtered_subject_name_str_list = pattern_match(subject_name_str_list, pattern)
        #
        return filtered_subject_name_str_list

    def sls(self, pattern=None, deleted=False):
        return self.get_subjects(pattern, deleted)

    def get_schema_versions(self, schema_id):
        schema_id_int = schema_id
        #
        url_str = f"{self.schema_registry_config_dict['schema.registry.url']}/schemas/ids/{schema_id_int}/versions"
        headers_dict = {"Accept": "application/json"}
        auth_str_tuple = None
        #
        if "basic.auth.credentials.source" in self.schema_registry_config_dict and self.schema_registry_config_dict["basic.auth.credentials.source"] == "USER_INFO":
            basic_auth_user_info_str = self.schema_registry_config_dict["basic.auth.user.info"]
            auth_str_tuple = tuple(basic_auth_user_info_str.split(":"))
        #
        schema_version_dict_list = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, debug_bool=self.verbose() >= 2)
        #
        return schema_version_dict_list

    def delete_subject(self, pattern, permanent=False):
        permanent_bool = permanent
        #
        subject_name_str_list = self.get_subjects(deleted=permanent_bool)
        filtered_subject_name_str_list = pattern_match(subject_name_str_list, pattern)
        #
        subject_name_str_schema_id_int_list_dict = {}
        for subject_name_str in filtered_subject_name_str_list:
            schema_id_int_list = self.schemaRegistryClient.delete_subject(subject_name_str, permanent_bool)
            subject_name_str_schema_id_int_list_dict[subject_name_str] = schema_id_int_list
        #
        return subject_name_str_schema_id_int_list_dict

    def srm(self, pattern, permanent=False):
        return self.delete_subject(pattern, permanent)

    def get_latest_version(self, subject_name):
        subject_name_str = subject_name
        #
        registeredSchema = self.schemaRegistryClient.get_latest_version(subject_name_str)
        registeredSchema_dict = registeredSchema_to_registeredSchema_dict(registeredSchema)
        #
        return registeredSchema_dict

    def get_version(self, subject_name, version, deleted=False):
        subject_name_str = subject_name
        version_int = version
        deleted_bool = deleted
        #
        registeredSchema = self.schemaRegistryClient.get_version(subject_name_str, version_int, deleted=deleted_bool)
        registeredSchema_dict = registeredSchema_to_registeredSchema_dict(registeredSchema)
        #
        return registeredSchema_dict

    def get_versions(self, subject_name):
        subject_name_str = subject_name
        #
        version_int_list = self.schemaRegistryClient.get_versions(subject_name_str)
        #
        return version_int_list

    def delete_version(self, subject_name, version, permanent=False):
        subject_name_str = subject_name
        version_int = version
        permanent_bool = permanent
        #
        schema_id_int = self.schemaRegistryClient.delete_version(subject_name_str, version_int, permanent=permanent_bool)
        #
        return schema_id_int

    def set_compatibility(self, subject_name, level):
        subject_name_str = subject_name
        level_str = level
        #
        set_level_dict = self.schemaRegistryClient.set_compatibility(subject_name_str, level_str)
        set_level_str = set_level_dict["compatibility"]
        #
        return set_level_str

    def set_comp(self, subject_name, level):
        return self.set_compatibility(subject_name, level)

    #

    def get_compatibility(self, subject_name):
        subject_name_str = subject_name
        #
        level_str = self.schemaRegistryClient.get_compatibility(subject_name_str)
        #
        return level_str

    def get_comp(self, subject_name):
        return self.get_compatibility(subject_name)

    #

    def test_compatibility(self, subject_name, schema, version="latest"):
        subject_name_str = subject_name
        schema_dict = schema
        version_str = version
        #
        schema1 = Schema(schema_dict["schema_str"], schema_dict["schema_type"]) # TODO: support references
        #
        compatible_bool = self.schemaRegistryClient.test_compatibility(subject_name_str, schema1, version_str)
        #
        return compatible_bool

    def test_comp(self, subject_name, schema, version="latest"):
        return self.test_compatibility(subject_name, schema, version)

#

def registeredSchema_to_registeredSchema_dict(registeredSchema):
    registeredSchema_dict = {"schema_id": registeredSchema.schema_id,
                             "schema": schema_to_schema_dict(registeredSchema.schema),
                             "subject": registeredSchema.subject,
                             "version": registeredSchema.version}
    #
    return registeredSchema_dict

def schema_to_schema_dict(schema):
    schema_dict = {"schema_str": schema.schema_str,
                   "schema_type": schema.schema_type} # TODO: support references
    #
    return schema_dict
