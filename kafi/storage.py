import glob
import os
import re

from piny import YamlLoader

from kafi.shell import Shell
from kafi.files import Files
from kafi.addons import AddOns
from kafi.schemaregistry import SchemaRegistry
from kafi.helpers import bytes_or_str_to_bytes, is_interactive

class Storage(Shell, Files, AddOns):
    def __init__(self, dir_str, config_str, mandatory_section_str_list, optional_section_str_list):
        self.dir_str = dir_str
        self.config_str = config_str
        self.mandatory_section_str_list = mandatory_section_str_list
        self.optional_section_str_list = optional_section_str_list
        #
        self.config_dict = self.get_config_dict(config_str)
        #
        self.schema_registry_config_dict = self.config_dict["schema_registry"] if "schema_registry" in self.config_dict else {}
        #
        self.schemaRegistry = None
        #
        self.kafi_config_dict = self.config_dict["kafi"] if "kafi" in self.config_dict else self.config_dict["kash"] if "kash" in self.config_dict else {}
        #
        if "progress.num.messages" not in self.kafi_config_dict:
            self.progress_num_messages(1000)
        else:
            self.progress_num_messages(int(self.kafi_config_dict["progress.num.messages"]))
        #
        if "read.batch.size" not in self.kafi_config_dict:
            self.consume_batch_size(1000)
        else:
            self.consume_batch_size(int(self.kafi_config_dict["read.batch.size"]))
        #
        if "write.batch.size" not in self.kafi_config_dict:
            self.produce_batch_size(1000)
        else:
            self.produce_batch_size(int(self.kafi_config_dict["write.batch.size"]))
        #
        if "verbose" not in self.kafi_config_dict:
            verbose_int = 1 if is_interactive() else 0
            self.verbose(verbose_int)
        else:
            self.verbose(int(self.kafi_config_dict["verbose"]))
        #
        if "auto.offset.reset" not in self.kafi_config_dict:
            self.auto_offset_reset("earliest")
        else:
            self.auto_offset_reset(str(self.kafi_config_dict["auto.offset.reset"]))
        #
        if "consumer.group.prefix" not in self.kafi_config_dict:
            self.consumer_group_prefix("")
        else:
            self.consumer_group_prefix(str(self.kafi_config_dict["consumer.group.prefix"]))
        #
        if "enable.auto.commit" not in self.kafi_config_dict:
            self.enable_auto_commit(False)
        else:
            self.enable_auto_commit(bool(self.kafi_config_dict["enable.auto.commit"]))
        #
        if "commit.after.processing" not in self.kafi_config_dict:
            self.commit_after_processing(True)
        else:
            self.commit_after_processing(bool(self.kafi_config_dict["commit.after.processing"]))
        #
        if "key.type" not in self.kafi_config_dict:
            self.key_type("str")
        else:
            self.key_type(str(self.kafi_config_dict["key.type"]))
        #
        if "value.type" not in self.kafi_config_dict:
            self.value_type("json")
        else:
            self.value_type(str(self.kafi_config_dict["value.type"]))
        #
        #
        #
        if "schema.registry.url" in self.schema_registry_config_dict:
            self.schemaRegistry = self.get_schemaRegistry()
        else:
            self.schemaRegistry = None

    #

    def progress_num_messages(self, new_value=None): # int
        return self.get_set_config("progress.num.messages", new_value)

    def consume_batch_size(self, new_value=None): # int
        return self.get_set_config("read.batch.size", new_value)
    
    def produce_batch_size(self, new_value=None): # int
        return self.get_set_config("write.batch.size", new_value)

    def verbose(self, new_value=None): # int
        return self.get_set_config("verbose", new_value)

    def auto_offset_reset(self, new_value=None): # str
        return self.get_set_config("auto.offset.reset", new_value)

    def consumer_group_prefix(self, new_value=None): # str
        return self.get_set_config("consumer.group.prefix", new_value)

    def enable_auto_commit(self, new_value=None): # bool
        return self.get_set_config("enable.auto.commit", new_value)

    def commit_after_processing(self, new_value=None): # bool
        return self.get_set_config("commit.after.processing", new_value)

    def key_type(self, new_value=None): # str
        return self.get_set_config("key.type", new_value)

    def value_type(self, new_value=None): # str
        return self.get_set_config("value.type", new_value)

    #

    def get_set_config(self, config_key_str, new_value=None, dict=None):
        dict = self.kafi_config_dict if dict is None else dict
        #
        if new_value is not None:
            dict[config_key_str] = new_value
        #
        return dict[config_key_str]

    #

    def get_config_dict(self, config_str):
        home_str = os.environ.get("KAFI_HOME")
        if not home_str:
            home_str = "."
        #
        configs_path_str = f"{home_str}/configs/{self.dir_str}"
        if os.path.exists(f"{configs_path_str}/{config_str}.yaml"):
            config_dict = YamlLoader(f"{configs_path_str}/{config_str}.yaml").load()
        elif os.path.exists(f"{configs_path_str}/{self.config_str}.yml"):
            config_dict = YamlLoader(f"{configs_path_str}/{config_str}.yml").load()
        else:
            raise Exception(f"No configuration file \"{config_str}.yaml\" or \"{config_str}.yml\" found in \"{configs_path_str}\" directory (hint: use KAFI_HOME environment variable to set the kafi home directory).")
        #
        for mandatory_section_str in self.mandatory_section_str_list:
            if mandatory_section_str not in config_dict:
                raise Exception(f"Connection configuration file \"{config_str}.yaml\" does not include a \"{mandatory_section_str}\" section.")
        #
        for optional_section_str in self.optional_section_str_list:
            if optional_section_str not in config_dict:
                config_dict[optional_section_str] = {}
        #
        return config_dict

    def configs(self, pattern="*", verbose=False):
        pattern_str = pattern
        verbose_bool = verbose
        #
        home_str = os.environ.get("KAFI_HOME")
        if not home_str:
            home_str = "."
        #
        configs_path_str = f"{home_str}/configs/{self.dir_str}"
        yaml_config_path_str_list = glob.glob(f"{configs_path_str}/{pattern_str}.yaml")
        yml_config_path_str_list = glob.glob(f"{configs_path_str}/{pattern_str}.yml")
        #
        yaml_config_str_list = [re.search(f"{configs_path_str}/(.*)\.yaml", yaml_config_path_str).group(1) for yaml_config_path_str in yaml_config_path_str_list if re.search(".*/(.*)\.yaml", yaml_config_path_str) is not None]
        yml_config_str_list = [re.search(".*/(.*)\.yml", yml_config_path_str).group(1) for yml_config_path_str in yml_config_path_str_list if re.search(".*/(.*)\.yml", yml_config_path_str) is not None]
        #
        config_str_list = yaml_config_str_list + yml_config_str_list
        #
        if verbose_bool:
            config_str_config_dict_dict = {config_str: self.get_config_dict(config_str) for config_str in config_str_list}
            return config_str_config_dict_dict
        else:
            config_str_list.sort()
            return config_str_list

    def is_headers_tuple_list(self, headers):
        return isinstance(headers, list) and len(headers) > 0 and all(isinstance(header_tuple, tuple) and len(header_tuple) == 2 and isinstance(header_tuple[0], str) for header_tuple in headers)


    def is_headers_dict(self, headers):
        return isinstance(headers, dict) and len(headers) > 0 and all(isinstance(header_key, str) for header_key in headers.keys())


    def is_headers(self, headers):
        return self.is_headers_tuple_list(headers) or self.is_headers_dict(headers)

    def headers_to_headers_str_bytes_tuple_list(self, headers):
        if headers is None:
            headers_str_bytes_tuple_list = None
        elif self.is_headers_tuple_list(headers):
            headers_str_bytes_tuple_list = [(header_tuple[0], bytes_or_str_to_bytes(header_tuple[1])) for header_tuple in headers]
        elif self.is_headers_dict(headers):
            headers_str_bytes_tuple_list = [(header_key_str, bytes_or_str_to_bytes(header_value_str_or_bytes)) for header_key_str, header_value_str_or_bytes in headers.items()]
        else:
            raise Exception("Type error: Headers must either be a list of tuples of strings and bytes, or a dictionary of strings and bytes.")
        #
        return headers_str_bytes_tuple_list

    #

    def get_schemaRegistry(self):
        schemaRegistry = SchemaRegistry(self.schema_registry_config_dict, self.kafi_config_dict)
        #
        return schemaRegistry

    # Helpers

    def get_key_value_type_tuple(self, **kwargs):
        # Default key and value types.
        key_type = self.key_type()
        value_type = self.value_type()
        #
        if "type" in kwargs:
            key_type = kwargs["type"]
            value_type = key_type
        #
        if "key_type" in kwargs:
            key_type = kwargs["key_type"]
        #
        if "value_type" in kwargs:
            value_type = kwargs["value_type"]
        #
        return (key_type, value_type)
