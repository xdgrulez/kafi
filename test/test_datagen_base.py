from kafi.streams.topologynode import (
    source
)

from datagen.shoe_clickstream import ShoeClickstreamGenerator
from datagen.shoe_customers import ShoeCustomerGenerator

#

class TestDatagenBase():
    def get_topology(self):
        click_topic_str = "shoe_clickstream"
        customer_topic_str = "shoe_customers"
        #
        source_click_topologyNode = source(click_topic_str)
        source_customer_topologyNode = source(customer_topic_str)
        #
        click_topologyNode = (
            source_click_topologyNode
            .map(lambda x: {"user_id": x["payload"]["user_id"], "ip": x["payload"]["ip"]})
        )
        #
        customer_topologyNode = (
            source_customer_topologyNode
            .map(lambda x: {"id": x["payload"]["id"], "first_name": x["payload"]["first_name"]})
        )
        #
        root_topologyNode = (
            click_topologyNode
            .join(customer_topologyNode,
                  left_on_function=lambda l: l["user_id"],
                  right_on_function=lambda r: r["id"],
                  projection_function=lambda l, r: {"user_id": l["user_id"],
                                                    "ip": l["ip"],
                                                    "first_name": r["first_name"]},
                  profile_config_dict = None)
        )
        #
        return root_topologyNode

    #

    def generate(self, source_str, batch_size_int):
        message_dict_list = []
        #
        if source_str == "shoe_clickstream":
            generator = ShoeClickstreamGenerator()
            #
            for _ in range(batch_size_int):
                record_dict = generator.generate_record()
                message_dict = {"key": None,
                                "value": {"payload": record_dict}}
                message_dict_list.append(message_dict)
        elif source_str == "shoe_customers":
            generator = ShoeCustomerGenerator()
            #
            for _ in range(batch_size_int):
                record_dict = generator.generate_record()
                message_dict = {"key": None,
                                "value": {"payload": record_dict}}
                message_dict_list.append(message_dict)
        #
        return message_dict_list 
