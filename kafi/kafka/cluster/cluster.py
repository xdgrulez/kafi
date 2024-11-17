from kafi.kafka.cluster.cluster_admin import ClusterAdmin
from kafi.kafka.cluster.cluster_consumer import ClusterConsumer
from kafi.kafka.cluster.cluster_producer import ClusterProducer
from kafi.kafka.kafka import Kafka
from kafi.helpers import is_interactive

# Cluster class

class Cluster(Kafka):
    def __init__(self, config_str):
        super().__init__("clusters", config_str, ["kafka"], ["schema_registry"])
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
        self.admin = self.get_admin()

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
