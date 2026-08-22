from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from kafi.helpers import pattern_match, get, delete

class SchemaRegistry:
    def __init__(self, schema_registry_config_dict):
        """Connect to the schema registry, if configured.

        Args:
            schema_registry_config_dict: schema.registry.url, basic.auth.user.info etc.; {} disables it"""
        self.schema_registry_config_dict = schema_registry_config_dict
        #
        if self.schema_registry_config_dict == {}:
            self.schemaRegistryClient = None
        else:
            self.schemaRegistryClient = self.get_schemaRegistryClient()

    def get_schemaRegistryClient(self):
        """Build a confluent_kafka SchemaRegistryClient from the stored config.

        Returns:
            SchemaRegistryClient: configured schema registry client"""
        dict = {}
        #
        dict["url"] = self.schema_registry_config_dict["schema.registry.url"]
        if "basic.auth.user.info" in self.schema_registry_config_dict:
            dict["basic.auth.user.info"] = self.schema_registry_config_dict["basic.auth.user.info"]
        #
        schemaRegistryClient = SchemaRegistryClient(dict)
        return schemaRegistryClient

    def get_schema(self, schema_id):
        """Fetch a schema by id.

        Args:
            schema_id: schema registry id

        Returns:
            dict: schema dict with schema_str and schema_type"""
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
        """Build a schema dict from raw schema source and its type.

        Args:
            schema_str: schema source (Avro/JSON Schema/Protobuf)
            schema_type_str: AVRO, JSON, or PROTOBUF

        Returns:
            dict: schema dict with schema_str and schema_type"""
        schema = Schema(schema_str, schema_type_str) # TODO: support references
        schema_dict = schema_to_schema_dict(schema)
        #
        return schema_dict

    def create_subject_name_str(self, topic_str, key_bool):
        """Build the schema registry subject name for a topic's key or value.

        Args:
            topic_str: topic name
            key_bool: True for the key subject, False for the value subject

        Returns:
            str: subject name"""
        key_or_value_str = "key" if key_bool else "value"
        #
        subject_name_str = f"{topic_str}-{key_or_value_str}"
        #
        return subject_name_str

    def register_schema(self, subject_name, schema, normalize=False):
        """Register a schema under a subject, returning its id.

        Args:
            subject_name: subject to register under
            schema: schema dict, as returned by create_schema_dict()
            normalize: whether to normalize before registering

        Returns:
            int: registered schema id"""
        subject_name_str = subject_name
        schema_dict = schema
        normalize_bool = normalize
        #
        schema1 = Schema(schema_dict["schema_str"], schema_dict["schema_type"]) # TODO: support references
        #
        schema_id_int = self.schemaRegistryClient.register_schema(subject_name_str, schema1, normalize_schemas=normalize_bool)
        return schema_id_int

    def lookup_schema(self, subject_name, schema, normalize=False):
        """Look up an already-registered schema under a subject.

        Args:
            subject_name: subject to look under
            schema: schema dict, as returned by create_schema_dict()
            normalize: whether to normalize before looking up

        Returns:
            dict: registered schema dict (schema_id, schema, subject, version, guid)"""
        subject_name_str = subject_name
        schema_dict = schema
        normalize_bool = normalize
        #
        schema1 = Schema(schema_dict["schema_str"], schema_dict["schema_type"]) # TODO: support references
        #
        registeredSchema = self.schemaRegistryClient.lookup_schema(subject_name_str, schema1, normalize_schemas=normalize_bool)
        registeredSchema_dict = registeredSchema_to_registeredSchema_dict(registeredSchema)
        #
        return registeredSchema_dict

    def get_subjects(self, pattern=None, deleted=False, only_deleted=False):
        """List subjects, optionally including soft-deleted ones.

        Args:
            pattern: glob pattern(s) matching subject names
            deleted: if True, include soft-deleted subjects
            only_deleted: if True, return only soft-deleted subjects

        Returns:
            list of str: matching subject names"""
        deleted_bool = deleted
        only_deleted_bool = only_deleted
        #
        def get_subjects_deleted():
            url_str = f"{self.schema_registry_config_dict['schema.registry.url']}/subjects?deleted=true"
            headers_dict = {"Accept": "application/json"}
            auth_str_tuple = None
            #
            if "basic.auth.credentials.source" in self.schema_registry_config_dict and self.schema_registry_config_dict ["basic.auth.credentials.source"] == "USER_INFO":
                basic_auth_user_info_str = self.schema_registry_config_dict["basic.auth.user.info"]
                auth_str_tuple = tuple(basic_auth_user_info_str.split(":"))
            #
            subject_name_str_list = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, debug_bool=self.verbose() >= 2)
            #
            return subject_name_str_list
        #
        if deleted_bool:
            subject_name_str_list = get_subjects_deleted()
        elif only_deleted_bool:
            subject_name_str_list = get_subjects_deleted()
            #
            non_deleted_subject_name_str_list = self.schemaRegistryClient.get_subjects()
            #
            subject_name_str_list = [subject_name_str for subject_name_str in subject_name_str_list if subject_name_str not in non_deleted_subject_name_str_list]
        else:
            subject_name_str_list = self.schemaRegistryClient.get_subjects()
        #
        filtered_subject_name_str_list = pattern_match(subject_name_str_list, pattern)
        #
        return filtered_subject_name_str_list

    def sls(self, pattern=None, deleted=False, only_deleted=False):
        """Alias for get_subjects().

        Returns:
            list of str: matching subject names"""
        return self.get_subjects(pattern, deleted, only_deleted)

    def get_schema_versions(self, schema_id):
        """List the (subject, version) pairs a schema id is registered under.

        Args:
            schema_id: schema registry id

        Returns:
            list of dict: {subject, version} pairs the schema id is registered under"""
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
        """Soft- (or, if permanent, hard-) delete subjects matching a pattern.

        Args:
            pattern: glob pattern(s) matching subject names
            permanent: if True, hard-delete (subject must already be soft-deleted)

        Returns:
            dict: {subject: [deleted schema ids]}"""
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
        """Alias for delete_subject().

        Returns:
            dict: {subject: [deleted schema ids]}"""
        return self.delete_subject(pattern, permanent)

    def delete_subject_force(self, pattern):
        """Soft-delete then hard-delete subjects matching a pattern, in one call.

        Args:
            pattern: glob pattern(s) matching subject names

        Returns:
            dict: {subject: [deleted schema ids]} from the hard-delete pass"""
        self.delete_subject(pattern)
        return self.delete_subject(pattern, permanent=True)

    def srmf(self, pattern):
        """Alias for delete_subject_force().

        Returns:
            dict: {subject: [deleted schema ids]} from the hard-delete pass"""
        return self.delete_subject_force(pattern)

    def get_latest_version(self, subject_name):
        """Fetch the latest registered schema version for a subject.

        Args:
            subject_name: subject to look up

        Returns:
            dict: registered schema dict (schema_id, schema, subject, version, guid)"""
        subject_name_str = subject_name
        #
        registeredSchema = self.schemaRegistryClient.get_latest_version(subject_name_str)
        registeredSchema_dict = registeredSchema_to_registeredSchema_dict(registeredSchema)
        #
        return registeredSchema_dict

    def get_version(self, subject_name, version, deleted=False):
        """Fetch a specific registered schema version for a subject.

        Args:
            subject_name: subject to look up
            version: version number to fetch
            deleted: if True, include soft-deleted versions

        Returns:
            dict: registered schema dict (schema_id, schema, subject, version, guid)"""
        subject_name_str = subject_name
        version_int = version
        deleted_bool = deleted
        #
        registeredSchema = self.schemaRegistryClient.get_version(subject_name_str, version_int, deleted=deleted_bool)
        registeredSchema_dict = registeredSchema_to_registeredSchema_dict(registeredSchema)
        #
        return registeredSchema_dict

    def get_versions(self, subject_name):
        """List all version numbers registered for a subject.

        Args:
            subject_name: subject to look up

        Returns:
            list of int: registered version numbers"""
        subject_name_str = subject_name
        #
        version_int_list = self.schemaRegistryClient.get_versions(subject_name_str)
        #
        return version_int_list

    # permanent=True is supported by confluent_kafka 2.8.0, but still has a bug...
    def delete_version(self, subject_name, version, permanent=False):
        """Soft- (or, if permanent, hard-) delete one version of a subject's schema.

        Args:
            subject_name: subject to delete from
            version: version number to delete
            permanent: if True, hard-delete

        Returns:
            int: deleted schema id"""
        subject_name_str = subject_name
        version_int = version
        permanent_bool = permanent
        #
        if permanent_bool:
            url_str = f"{self.schema_registry_config_dict['schema.registry.url']}/subjects/{subject_name_str}/versions/{version_int}?permanent=true"
            headers_dict = {"Accept": "application/json"}
            auth_str_tuple = None
            #
            if "basic.auth.credentials.source" in self.schema_registry_config_dict and self.schema_registry_config_dict["basic.auth.credentials.source"] == "USER_INFO":
                basic_auth_user_info_str = self.schema_registry_config_dict["basic.auth.user.info"]
                auth_str_tuple = tuple(basic_auth_user_info_str.split(":"))
            #
            schema_id_int = delete(url_str, headers_dict, auth_str_tuple, debug_bool=self.verbose() >= 2)
        else:
            schema_id_int = self.schemaRegistryClient.delete_version(subject_name_str, version_int)
        #
        return schema_id_int

    def set_compatibility(self, subject_name, level):
        """Set the compatibility level for a subject.

        Args:
            subject_name: subject to configure
            level: compatibility level, e.g. BACKWARD, FULL, NONE

        Returns:
            str: applied compatibility level"""
        subject_name_str = subject_name
        level_str = level
        #
        set_level_dict = self.schemaRegistryClient.set_compatibility(subject_name_str, level_str)
        set_level_str = set_level_dict["compatibility"]
        #
        return set_level_str

    def set_comp(self, subject_name, level):
        """Alias for set_compatibility().

        Returns:
            str: applied compatibility level"""
        return self.set_compatibility(subject_name, level)

    #

    def get_compatibility(self, subject_name):
        """Get the compatibility level for a subject.

        Args:
            subject_name: subject to query

        Returns:
            str: compatibility level"""
        subject_name_str = subject_name
        #
        level_str = self.schemaRegistryClient.get_compatibility(subject_name_str)
        #
        return level_str

    def get_comp(self, subject_name):
        """Alias for get_compatibility().

        Returns:
            str: compatibility level"""
        return self.get_compatibility(subject_name)

    #

    def get_schema_by_guid(self, guid, fmt=None):
        """Fetch a schema by its GUID.

        Args:
            guid: schema registry GUID
            fmt: optional response format

        Returns:
            dict: schema dict with schema_str and schema_type"""
        schema = self.schemaRegistryClient.get_schema_by_guid(guid, fmt)
        #
        schema_dict = schema_to_schema_dict(schema)
        #
        return schema_dict

    #

    def test_compatibility(self, subject_name, schema, version="latest"):
        """Check whether a schema is compatible with a subject's registered version.

        Args:
            subject_name: subject to check against
            schema: schema dict to test, as returned by create_schema_dict()
            version: version to test against, or latest

        Returns:
            bool: True if compatible"""
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
        """Alias for test_compatibility().

        Returns:
            bool: True if compatible"""
        return self.test_compatibility(subject_name, schema, version)

#

def registeredSchema_to_registeredSchema_dict(registeredSchema):
    """Convert a confluent_kafka RegisteredSchema object into a plain dict.

    Args:
        registeredSchema: confluent_kafka RegisteredSchema instance

    Returns:
        dict: {schema_id, schema, subject, version, guid}"""
    registeredSchema_dict = {"schema_id": registeredSchema.schema_id,
                             "schema": schema_to_schema_dict(registeredSchema.schema),
                             "subject": registeredSchema.subject,
                             "version": registeredSchema.version,
                             "guid": registeredSchema.guid}
    #
    return registeredSchema_dict

def schema_to_schema_dict(schema):
    """Convert a confluent_kafka Schema object into a plain dict.

    Args:
        schema: confluent_kafka Schema instance

    Returns:
        dict: {schema_str, schema_type}"""
    schema_dict = {"schema_str": schema.schema_str,
                   "schema_type": schema.schema_type} # TODO: support references
    #
    return schema_dict
