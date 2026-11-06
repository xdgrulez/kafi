from kafi.storage import Storage

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
        if "cluster.kind" not in self.kafi_config_dict:
            self.cluster_kind("kafka")
        else:
            self.cluster_kind(str(self.kafi_config_dict["cluster.kind"]))
        #
        if "retention.ms" not in self.kafi_config_dict:
            self.retention_ms(604800000)
        else:
            self.retention_ms(int(self.kafi_config_dict["retention.ms"]))

    #

    def cluster_kind(self, new_value=None): # str
        return self.get_set_config("cluster.kind", new_value)

    def retention_ms(self, new_value=None): # int
        return self.get_set_config("retention.ms", new_value)

    # Brokers

    def brokers(self, pattern=None):
        return self.admin.brokers(pattern)
    
    def broker_config(self, pattern=None, config=None, **kwargs):
        return self.admin.broker_config(pattern, config, **kwargs)
    
    # ACLs

    def acls(self, acl_dict={}):
        return self.admin.acls(acl_dict)

    def create_acl(self, acl_dict):
        return self.admin.create_acl(acl_dict)
    
    def delete_acls(self, acl_dict):
        return self.admin.delete_acls(acl_dict)
