from kafi.kafka.cluster.cluster_admin import ClusterAdmin
from kafi.kafka.cluster.cluster_consumer import ClusterConsumer
from kafi.kafka.cluster.cluster_producer import ClusterProducer
from kafi.kafka.kafka import Kafka
from kafi.helpers import is_interactive

# Cluster class

class Cluster(Kafka):
    def __init__(self, config_str_or_dict):
        super().__init__("clusters", config_str_or_dict, ["kafka"], ["schema_registry"])
        #
        # If not set already in the config, set librdkafka log level to:
        # * 3 ("Error") if interactive (=in the interpreter)
        # * 6 ("Notice", default for librdkafka) otherwise
        # See https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
        # and https://en.wikipedia.org/wiki/Syslog
        if "log_level" not in self.kafka_config_dict:
            if is_interactive():
                self.kafka_config_dict["log_level"] = 3
            else:
                self.kafka_config_dict["log_level"] = 6
        #
        if "cluster.consume.timeout" not in self.kafi_config_dict:
            self.cluster_consume_timeout(0.1)
        else:
            self.cluster_consume_timeout(float(self.kafi_config_dict["cluster.consume.timeout"]))
        #
        if "flush.timeout" not in self.kafi_config_dict:
            self.flush_timeout(-1.0)
        else:
            self.flush_timeout(float(self.kafi_config_dict["flush.timeout"]))
        #
        if "isolation.level" not in self.kafi_config_dict:
            self.isolation_level("read_uncommitted")
        else:
            self.isolation_level(str(self.kafi_config_dict["isolation.level"]))
        #
        if "session.timeout.ms" not in self.kafi_config_dict:
            self.session_timeout_ms(45000)
        else:
            self.session_timeout_ms(int(self.kafi_config_dict["session.timeout.ms"]))
        #
        if "transactional.id.prefix" not in self.kafi_config_dict:
            self.transactional_id_prefix("")
        else:
            self.transactional_id_prefix(str(self.kafi_config_dict["transactional.id.prefix"]))
        #
        self.admin = self.get_admin()

    #

    def cluster_consume_timeout(self, new_value=None): # float
        return self.get_set_config("cluster.consume.timeout", new_value)

    def flush_timeout(self, new_value=None): # float
        return self.get_set_config("flush.timeout", new_value)

    def isolation_level(self, new_value=None): # str
        return self.get_set_config("isolation.level", new_value)

    def session_timeout_ms(self, new_value=None): # int
        return self.get_set_config("session.timeout.ms", new_value)

    def transactional_id_prefix(self, new_value=None): # str
        return self.get_set_config("transactional.id.prefix", new_value)

    #

    def get_admin(self):
        admin = ClusterAdmin(self)
        #
        return admin

    #
    def get_consumer(self, topics, **kwargs):
        consumer = ClusterConsumer(self, topics, **kwargs)
        #
        return consumer

    #

    def get_producer(self, topic, **kwargs):
        producer = ClusterProducer(self, topic, **kwargs)
        #
        return producer
