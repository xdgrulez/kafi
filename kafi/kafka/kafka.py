from kafi.storage import Storage

# Constants

ALL_MESSAGES = -1

#

class Kafka(Storage):
    def __init__(self, config_dir_str, config_name_str, mandatory_section_str_list, optional_section_str_list):
        super().__init__(config_dir_str, config_name_str, mandatory_section_str_list, optional_section_str_list)
        #
        self.config_dir_str = config_dir_str
        self.config_name_str = config_name_str
        #
        if "kafka" in mandatory_section_str_list:
            self.kafka_config_dict = self.config_dict["kafka"]
        else:
            self.kafka_config_dict = None
        #
        if "rest_proxy" in mandatory_section_str_list:
            self.rest_proxy_config_dict = self.config_dict["rest_proxy"]
        else:
            self.rest_proxy_config_dict = None
        #
        self.admin = None
        #
        # cluster config kafi section
        #
        if "flush.timeout" not in self.kafi_config_dict:
            self.flush_timeout(-1.0)
        else:
            self.flush_timeout(float(self.kafi_config_dict["flush.timeout"]))
        #
        if "retention.ms" not in self.kafi_config_dict:
            self.retention_ms(604800000)
        else:
            self.retention_ms(int(self.kafi_config_dict["retention.ms"]))
        #
        if "consume.timeout" not in self.kafi_config_dict:
            self.consume_timeout(5.0)
        else:
            self.consume_timeout(float(self.kafi_config_dict["consume.timeout"]))
        #
        if "session.timeout.ms" not in self.kafi_config_dict:
            self.session_timeout_ms(45000)
        else:
            self.session_timeout_ms(int(self.kafi_config_dict["session.timeout.ms"]))
        #
        if "block.num.retries" not in self.kafi_config_dict:
            self.block_num_retries(10)
        else:
            self.block_num_retries(int(self.kafi_config_dict["block.num.retries"]))
        #
        if "block.interval" not in self.kafi_config_dict:
            self.block_interval(0.5)
        else:
            self.block_interval(float(self.kafi_config_dict["block.interval"]))
        #
        # both cluster and restproxy kafi section
        #
        if "fetch.min.bytes" not in self.kafi_config_dict:
            self.fetch_min_bytes(-1)
        else:
            self.fetch_min_bytes(int(self.kafi_config_dict["fetch.min.bytes"]))
        #
        if "consumer.request.timeout.ms" not in self.kafi_config_dict:
            self.consumer_request_timeout_ms(1000)
        else:
            self.consumer_request_timeout_ms(int(self.kafi_config_dict["consumer.request.timeout.ms"]))
        #
        if "consume.num.attempts" not in self.kafi_config_dict:
            self.consume_num_attempts(3)
        else:
            self.consume_num_attempts(int(self.kafi_config_dict["consume.num.attempts"]))
        #
        if "requests.num.retries" not in self.kafi_config_dict:
            self.requests_num_retries(10)
        else:
            self.requests_num_retries(int(self.kafi_config_dict["requests.num.retries"]))

    # Cluster

    def flush_timeout(self, new_value=None): # float
        return self.get_set_config("flush.timeout", new_value)

    def retention_ms(self, new_value=None): # int
        return self.get_set_config("retention.ms", new_value)

    def consume_timeout(self, new_value=None): # float
        return self.get_set_config("consume.timeout", new_value)

    def enable_auto_commit(self, new_value=None): # bool
        return self.get_set_config("enable.auto.commit", new_value)

    def session_timeout_ms(self, new_value=None): # int
        return self.get_set_config("session.timeout.ms", new_value)

    def block_num_retries(self, new_value=None): # int
        return self.get_set_config("block.num.retries", new_value)

    def block_interval(self, new_value=None): # float
        return self.get_set_config("block.interval", new_value)

    # RestProxy

    def fetch_min_bytes(self, new_value=None): # int
        return self.get_set_config("fetch.min.bytes", new_value)

    def consumer_request_timeout_ms(self, new_value=None): # int
        return self.get_set_config("consumer.request.timeout.ms", new_value)

    def consume_num_attempts(self, new_value=None): # int
        return self.get_set_config("consume.num.attempts", new_value)

    def requests_num_retries(self, new_value=None): # int
        return self.get_set_config("requests.num.retries", new_value)

    # Brokers

    def brokers(self, pattern=None):
        return self.admin.brokers(pattern)
    
    def broker_config(self, pattern=None, config=None, **kwargs):
        return self.admin.broker_config(pattern, config, **kwargs)
    
    # ACLs

    def acls(self, name=None, restype="any", resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        return self.admin.acls(name, restype, resource_pattern_type, principal, host, operation, permission_type)

    def create_acl(self, name=None, restype="any", resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        return self.admin.create_acl(name, restype, resource_pattern_type, principal, host, operation, permission_type)
    
    def delete_acl(self, name=None, restype="any", resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        return self.admin.delete_acl(name, restype, resource_pattern_type, principal, host, operation, permission_type)
