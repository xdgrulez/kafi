from kafi.kafka.restproxy.restproxy_admin import RestProxyAdmin
from kafi.kafka.restproxy.restproxy_consumer import RestProxyConsumer
from kafi.kafka.restproxy.restproxy_producer import RestProxyProducer
from kafi.kafka.kafka import Kafka
from kafi.helpers import get

# RestProxy class

class RestProxy(Kafka):
    def __init__(self, config_str_or_dict):
        super().__init__("restproxies", config_str_or_dict, ["rest_proxy"], ["schema_registry"])
        #
        self.cluster_id_str = self.get_cluster_id()
        #
        self.admin = self.get_admin()

    #

    def get_admin(self):
        admin = RestProxyAdmin(self)
        #
        return admin

    #

    def get_consumer(self, topics, **kwargs):
        consumer = RestProxyConsumer(self, topics, **kwargs)
        #
        return consumer

    #

    def get_producer(self, topics, **kwargs):
        producer = RestProxyProducer(self, topics, **kwargs)
        #
        return producer
    
    #

    def get_cluster_id(self):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters"
        headers_dict = {"Content-Type": "application/json"}
        auth_str_tuple = self.get_auth_str_tuple()
        response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries_int=self.kafi_config_dict["requests.num.retries"])
        #
        cluster_id_str = response_dict["data"][0]["cluster_id"]
        return cluster_id_str

    #

    def get_auth_str_tuple(self):
        if "basic.auth.user.info" in self.rest_proxy_config_dict:
            return tuple(self.rest_proxy_config_dict["basic.auth.user.info"].split(":"))
        else:
            return None
        
    #

    def get_url_str_auth_str_tuple_tuple(self):
        return (self.rest_proxy_config_dict["rest.proxy.url"], self.get_auth_str_tuple())
