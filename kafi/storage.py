import glob
import os
from pathlib import Path

from piny import YamlLoader

from kafi.shell import Shell
from kafi.files import Files
from kafi.addons import AddOns
from kafi.schemaregistry import SchemaRegistry
from kafi.helpers import bytes_or_str_to_bytes, hash_dict, is_interactive, pattern_match

class Storage(Shell, Files, AddOns, SchemaRegistry):
    def __init__(self, dir_str, config_str_or_dict, mandatory_section_str_list, optional_section_str_list):
        """Load config, apply defaults, and initialize schema registry for this storage.

        Args:
            dir_str: config subdirectory for this storage kind, e.g. "kafka" or "local"
            config_str_or_dict: config name (looked up under KAFI_HOME/configs/dir_str) or an inline config dict
            mandatory_section_str_list: config sections that must be present
            optional_section_str_list: config sections filled in as {} if missing"""
        self.dir_str = dir_str
        self.config_str = config_str_or_dict if isinstance(config_str_or_dict, str) else hash_dict(config_str_or_dict)
        self.mandatory_section_str_list = mandatory_section_str_list
        self.optional_section_str_list = optional_section_str_list
        #
        self.config_dict = config_str_or_dict if isinstance(config_str_or_dict, dict) else self.get_config_dict(config_str_or_dict)
        #
        self.schema_registry_config_dict = self.config_dict["schema_registry"] if "schema_registry" in self.config_dict else {}
        #
        self.kafi_config_dict = self.config_dict["kafi"] if "kafi" in self.config_dict else {}
        #
        # if "schema.registry.url" in self.schema_registry_config_dict:
        SchemaRegistry.__init__(self, self.schema_registry_config_dict)
        #
        if "progress.num.messages" not in self.kafi_config_dict:
            self.progress_num_messages(1000)
        else:
            self.progress_num_messages(int(self.kafi_config_dict["progress.num.messages"]))
        #
        if "consume.batch.size" not in self.kafi_config_dict:
            self.consume_batch_size(1000)
        else:
            self.consume_batch_size(int(self.kafi_config_dict["consume.batch.size"]))
        #
        if "produce.batch.size" not in self.kafi_config_dict:
            self.produce_batch_size(1000)
        else:
            self.produce_batch_size(int(self.kafi_config_dict["produce.batch.size"]))
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
        if "isolation.level" not in self.kafi_config_dict:
            self.isolation_level("read_uncommitted")
        else:
            self.isolation_level(str(self.kafi_config_dict["isolation.level"]))
        #
        if "transactional.id.prefix" not in self.kafi_config_dict:
            self.transactional_id_prefix("")
        else:
            self.transactional_id_prefix(str(self.kafi_config_dict["transactional.id.prefix"]))
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
        if "cluster.kind" not in self.kafi_config_dict:
            self.cluster_kind("kafka")
        else:
            self.cluster_kind(str(self.kafi_config_dict["cluster.kind"]))
        #
        if "topic.ignore.patterns" not in self.kafi_config_dict:
            self.topic_ignore_patterns(["_*"])
        else:
            self.topic_ignore_patterns(list(self.kafi_config_dict["topic.ignore.patterns"]))

    #

    def progress_num_messages(self, new_value=None): # int
        """Get/set how many messages trigger a progress print (int).

        Returns:
            int: current value"""
        return self.get_set_config("progress.num.messages", new_value)

    def consume_batch_size(self, new_value=None): # int
        """Get/set the batch size used when consuming (int).

        Returns:
            int: current value"""
        return self.get_set_config("consume.batch.size", new_value)
    
    def produce_batch_size(self, new_value=None): # int
        """Get/set the batch size used when producing (int).

        Returns:
            int: current value"""
        return self.get_set_config("produce.batch.size", new_value)

    def verbose(self, new_value=None): # int
        """Get/set the verbosity level (int).

        Returns:
            int: current value"""
        return self.get_set_config("verbose", new_value)

    def auto_offset_reset(self, new_value=None): # str
        """Get/set the default consumer offset reset policy, e.g. "earliest" (str).

        Returns:
            str: current value"""
        return self.get_set_config("auto.offset.reset", new_value)

    def consumer_group_prefix(self, new_value=None): # str
        """Get/set the prefix prepended to auto-generated consumer group names (str).

        Returns:
            str: current value"""
        return self.get_set_config("consumer.group.prefix", new_value)

    def enable_auto_commit(self, new_value=None): # bool
        """Get/set whether consumers auto-commit offsets (bool).

        Returns:
            bool: current value"""
        return self.get_set_config("enable.auto.commit", new_value)

    def commit_after_processing(self, new_value=None): # bool
        """Get/set whether offsets are committed after (True) or before (False) processing (bool).

        Returns:
            bool: current value"""
        return self.get_set_config("commit.after.processing", new_value)

    def isolation_level(self, new_value=None): # str
        """Get/set the consumer transaction isolation level, e.g. "read_uncommitted" (str).

        Returns:
            str: current value"""
        return self.get_set_config("isolation.level", new_value)

    def transactional_id_prefix(self, new_value=None): # str
        """Get/set the prefix prepended to auto-generated transactional ids (str).

        Returns:
            str: current value"""
        return self.get_set_config("transactional.id.prefix", new_value)

    def key_type(self, new_value=None): # str
        """Get/set the default (de)serialization type for keys, e.g. "str" (str).

        Returns:
            str: current value"""
        return self.get_set_config("key.type", new_value)

    def value_type(self, new_value=None): # str
        """Get/set the default (de)serialization type for values, e.g. "json" (str).

        Returns:
            str: current value"""
        return self.get_set_config("value.type", new_value)

    def cluster_kind(self, new_value=None): # str
        """Get/set the underlying cluster kind, e.g. "kafka" (str).

        Returns:
            str: current value"""
        return self.get_set_config("cluster.kind", new_value)

    def topic_ignore_patterns(self, new_value=None): # bool
        """Get/set glob patterns for topics hidden from topic listings (list of str).

        Returns:
            list of str: current value"""
        return self.get_set_config("topic.ignore.patterns", new_value)

    #

    def get_set_config(self, config_key_str, new_value=None, dict=None):
        """Get, or set and get, a single config value.

        Args:
            config_key_str: dotted config key, e.g. "verbose"
            new_value: if not None, stored under config_key_str before returning
            dict: config dict to read/write; defaults to self.kafi_config_dict

        Returns:
            any: the (possibly newly set) config value"""
        dict = self.kafi_config_dict if dict is None else dict
        #
        if new_value is not None:
            dict[config_key_str] = new_value
        #
        return dict[config_key_str]

    #

    def get_config_dict(self, config_str):
        """Load a YAML config file by name from the KAFI_HOME config path, and validate its sections.

        Args:
            config_str: config file name (without .yaml/.yml extension)

        Returns:
            dict: loaded and validated config"""
        home_str = os.environ.get("KAFI_HOME")
        if not home_str:
            home_str = "."
        #
        config_dict = None
        configs_path_str_list = [f"{home_str}/configs/{self.dir_str}", f"{home_str}/configs", f"{home_str}"]
        for configs_path_str in configs_path_str_list:
            if os.path.exists(f"{configs_path_str}/{config_str}.yaml"):
                config_dict = YamlLoader(f"{configs_path_str}/{config_str}.yaml").load()
            elif os.path.exists(f"{configs_path_str}/{config_str}.yml"):
                config_dict = YamlLoader(f"{configs_path_str}/{config_str}.yml").load()
        if config_dict is None:
            raise Exception(f"No configuration file \"{config_str}.yaml\" or \"{config_str}.yml\" found in \"{configs_path_str_list}\" (hint: you can use KAFI_HOME environment variable to set the kafi home directory).")
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
        """List (or load) available config files for this storage kind.

        Args:
            pattern: glob pattern over config names
            verbose: if True, return {config_str: config_dict} instead of just names

        Returns:
            list of str, or dict: sorted config names, or {config name: config dict} if verbose"""
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
        yml_config_str_list = [Path(yml_config_path_str).stem for yml_config_path_str in yml_config_path_str_list if yml_config_path_str and Path(yml_config_path_str).suffix == ".yml"]
        yaml_config_str_list = [Path(yaml_config_path_str).stem for yaml_config_path_str in yaml_config_path_str_list if yaml_config_path_str and Path(yaml_config_path_str).suffix == ".yaml"]
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
        """True if headers is a non-empty list of (str, value) tuples.

        Returns:
            bool: True if headers is a non-empty list of (str, value) tuples"""
        return isinstance(headers, list) and len(headers) > 0 and all(isinstance(header_tuple, tuple) and len(header_tuple) == 2 and isinstance(header_tuple[0], str) for header_tuple in headers)

    def is_headers_dict(self, headers):
        """True if headers is a non-empty dict with str keys.

        Returns:
            bool: True if headers is a non-empty dict with str keys"""
        return isinstance(headers, dict) and len(headers) > 0 and all(isinstance(header_key, str) for header_key in headers.keys())

    def is_headers_list_list(self, headers):
        """True if headers is a non-empty list of [str, value] 2-element lists.

        Returns:
            bool: True if headers is a non-empty list of [str, value] 2-element lists"""
        return isinstance(headers, list) and len(headers) > 0 and all(isinstance(header_list, list) and len(header_list) == 2 and isinstance(header_list[0], str) for header_list in headers)

    def is_headers(self, headers):
        """True if headers is None or any of the accepted header shapes.

        Returns:
            bool: True if headers is None or any of the accepted header shapes"""
        return headers == None or self.is_headers_tuple_list(headers) or self.is_headers_dict(headers) or self.is_headers_list_list(headers)

    def headers_to_headers_str_bytes_tuple_list(self, headers):
        """Normalize headers (dict, tuple-list, or list-list) into a list of (str, bytes).

        Args:
            headers: message headers in any of the accepted shapes, or None

        Returns:
            list of tuple, or None: [(str, bytes), ...] (None if headers was None)"""
        if headers is None:
            headers_str_bytes_tuple_list = None
        elif self.is_headers_tuple_list(headers):
            headers_str_bytes_tuple_list = [(header_tuple[0], bytes_or_str_to_bytes(header_tuple[1])) for header_tuple in headers]
        elif self.is_headers_list_list(headers):
            headers_str_bytes_tuple_list = [(header_tuple[0], bytes_or_str_to_bytes(header_tuple[1])) for header_tuple in headers]
        elif self.is_headers_dict(headers):
            headers_str_bytes_tuple_list = [(header_key_str, bytes_or_str_to_bytes(header_value_str_or_bytes)) for header_key_str, header_value_str_or_bytes in headers.items()]
        else:
            raise Exception("Type error: Headers must either be a list of tuples of strings and bytes, or a dictionary of strings and bytes.")
        #
        return headers_str_bytes_tuple_list

    #

    def get_id(self):
        """Unique id for this storage: (dir_str, config_str).

        Returns:
            tuple: (dir_str, config_str)"""
        return (self.dir_str, self.config_str)

    # Topics

    def topics(self, pattern=None, size=False, **kwargs):
        """List topics (and optionally their size), delegating to the admin client.

        Args:
            pattern: glob pattern(s) matching topic names; None matches all
            size: if True, include per-topic size
            **kwargs: passed through to the admin client

        Returns:
            as returned by admin.topics(): list of str, or dict, depending on size/partitions kwargs"""
        return self.admin.topics(pattern, size, **kwargs)
    
    ls = topics

    def l(self, pattern=None, size=True, **kwargs):
        """Alias for topics() with size=True by default.

        Returns:
            as returned by admin.topics(): dict of topic sizes (and partitions, if requested)"""
        return self.admin.topics(pattern=pattern, size=size, **kwargs)

    ll = l

    def exists(self, topic):
        """Whether a topic exists.

        Args:
            topic: topic name

        Returns:
            bool: True if the topic exists"""
        topic_str = topic
        #
        return self.admin.topics(topic_str) != []

    #

    def watermarks(self, pattern, **kwargs):
        """Low/high watermark offsets per partition, delegating to the admin client.

        Args:
            pattern: glob pattern(s) matching topic names
            **kwargs: passed through to the admin client

        Returns:
            as returned by admin.watermarks(): dict of {topic: {partition: (low, high)}}"""
        return self.admin.watermarks(pattern, **kwargs)

    def delete_records(self, pattern_or_offsets, **kwargs):
        """Delete records up to given offsets, delegating to the admin client.

        Args:
            pattern_or_offsets: topic pattern, or explicit {topic: {partition: offset}} map
            **kwargs: passed through to the admin client

        Returns:
            as returned by admin.delete_records()"""
        return self.admin.delete_records(pattern_or_offsets, **kwargs)

    def lags(self, group_pattern, topic_pattern, **kwargs):
      """Consumer group lag per topic/partition (watermark minus committed offset).

      Args:
          group_pattern: glob pattern(s) matching consumer group names
          topic_pattern: glob pattern(s) matching topic names
          **kwargs: passed through to the admin client's watermarks()

      Returns:
          dict: {group: {topic: {partition: lag}}}"""
      group_offsets = self.admin.group_offsets(group_pattern)
      topic_str_partition_int_offsets_tuple_dict_dict = self.admin.watermarks(topic_pattern, **kwargs)
      #
      group_str_topic_str_lags_dict_dict_dict = {}
      for group_str, topic_str_group_offsets_dict_dict in group_offsets.items():
          group_str_topic_str_lags_dict_dict_dict[group_str] = {}
          for topic_str, group_offsets_dict in topic_str_group_offsets_dict_dict.items():
              group_str_topic_str_lags_dict_dict_dict[group_str][topic_str] = {partition_int: topic_str_partition_int_offsets_tuple_dict_dict[topic_str][partition_int][1] - group_offset_int for partition_int, group_offset_int in group_offsets_dict.items()}
      #
      return group_str_topic_str_lags_dict_dict_dict

    def config(self, pattern, config=None, **kwargs):
        """Get or set topic-level configuration, delegating to the admin client.

        Args:
            pattern: glob pattern(s) matching topic names
            config: dict of config overrides to apply; None to just read
            **kwargs: passed through to the admin client

        Returns:
            as returned by admin.config()"""
        return self.admin.config(pattern, config, **kwargs)

    def create(self, topic, partitions=1, **kwargs):
        """Create a topic, delegating to the admin client.

        Args:
            topic: topic name
            partitions: number of partitions
            **kwargs: passed through to the admin client

        Returns:
            as returned by admin.create()"""
        return self.admin.create(topic, partitions, **kwargs)
    
    touch = create

    def delete(self, pattern, **kwargs):
        """Delete topic(s) matching a pattern, delegating to the admin client.

        Args:
            pattern: glob pattern(s) matching topic names
            **kwargs: passed through to the admin client

        Returns:
            as returned by admin.delete()"""
        return self.admin.delete(pattern, **kwargs)

    rm = delete

    def offsets_for_times(self, pattern, partitions_timestamps, **kwargs):
        """Resolve offsets for given timestamps per partition, delegating to the admin client.

        Args:
            pattern: glob pattern(s) matching topic names
            partitions_timestamps: {partition: timestamp_ms} to resolve
            **kwargs: passed through to the admin client

        Returns:
            as returned by admin.offsets_for_times()"""
        return self.admin.offsets_for_times(pattern, partitions_timestamps, **kwargs)

    def partitions(self, pattern, partitions=None, verbose=False, **kwargs):
        """Get or set the number of partitions, delegating to the admin client.

        Args:
            pattern: glob pattern(s) matching topic names
            partitions: new partition count; None to just read
            verbose: if True, return more detail
            **kwargs: passed through to the admin client

        Returns:
            as returned by admin.partitions()"""
        return self.admin.partitions(pattern, partitions, verbose, **kwargs)

    # Groups

    def groups(self, pattern="*", state_pattern="*", state=False):
        """List consumer groups, delegating to the admin client.

        Args:
            pattern: glob pattern(s) matching group names
            state_pattern: glob pattern(s) matching group state
            state: if True, include group state

        Returns:
            as returned by admin.groups()"""
        return self.admin.groups(pattern, state_pattern, state)
    
    gls = groups

    def describe_groups(self, pattern="*", state_pattern="*"):
        """Describe consumer groups, delegating to the admin client.

        Args:
            pattern: glob pattern(s) matching group names
            state_pattern: glob pattern(s) matching group state

        Returns:
            as returned by admin.describe_groups()"""
        return self.admin.describe_groups(pattern, state_pattern)
    
    def delete_groups(self, pattern, state_pattern="*"):
        """Delete consumer groups matching a pattern, delegating to the admin client.

        Args:
            pattern: glob pattern(s) matching group names
            state_pattern: glob pattern(s) matching group state

        Returns:
            as returned by admin.delete_groups()"""
        return self.admin.delete_groups(pattern, state_pattern)
    
    grm = delete_groups

    def group_offsets(self, pattern, group_offsets=None, state_pattern="*"):
        """Get or set committed offsets per consumer group, delegating to the admin client.

        Args:
            pattern: glob pattern(s) matching group names
            group_offsets: offsets to commit; None to just read
            state_pattern: glob pattern(s) matching group state

        Returns:
            as returned by admin.group_offsets()"""
        return self.admin.group_offsets(pattern, group_offsets, state_pattern)

    #

    def consumer(self, topic, **kwargs):
        """Create a consumer for a topic.

        Args:
            topic: topic name
            **kwargs: passed to get_consumer()

        Returns:
            consumer: storage-specific consumer instance"""
        consumer = self.get_consumer(topic, **kwargs)
        #
        return consumer
    
    def producer(self, topic, **kwargs):
        """Create a producer for a topic.

        Args:
            topic: topic name
            **kwargs: passed to get_producer()

        Returns:
            producer: storage-specific producer instance"""
        producer = self.get_producer(topic, **kwargs)
        #
        return producer

    # Helpers

    def get_key_value_type_tuple(self, **kwargs):
        """Resolve the (key_type, value_type) to use, applying kwargs overrides over the defaults.

        Args:
            **kwargs: optional type (sets both key_type and value_type), key_type, value_type

        Returns:
            tuple: (key_type, value_type)"""
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

    def filter_topics(self, topic_str_list, pattern_str_or_str_list):
        """Match topic names against a pattern, minus any topic_ignore_patterns().

        Args:
            topic_str_list: candidate topic names
            pattern_str_or_str_list: glob pattern(s) to match against

        Returns:
            list of str: matching topic names, with internally-ignored ones excluded"""
        matched_topic_str_list = pattern_match(topic_str_list, pattern_str_or_str_list)
        #
        to_be_ignored_topic_str_list = pattern_match(topic_str_list, self.topic_ignore_patterns())
        #
        return [topic_str for topic_str in matched_topic_str_list if topic_str not in to_be_ignored_topic_str_list]
