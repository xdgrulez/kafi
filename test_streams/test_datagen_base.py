from kafi.streams.topologynode import (
    Runner
)

from datagen.shoe_clickstream import ShoeClickstreamGenerator
from datagen.shoe_customers import ShoeCustomerGenerator
from datagen.shoes import ShoeProductGenerator 
from datagen.shoe_orders import ShoeOrderGenerator

#

class TestDatagenBase():
    def get_runner_1_join(self, click_source_str, customer_source_str):
        runner = Runner()
        #
        source_click_topologyNode = runner.source(click_source_str)
        source_customer_topologyNode = runner.source(customer_source_str)
        #
        click_topologyNode = (
            source_click_topologyNode
            .map(lambda x: {"user_id": x["payload"]["user_id"], "ip": x["payload"]["ip"]})
            .distinct()
        )
        #
        customer_topologyNode = (
            source_customer_topologyNode
            .map(lambda x: {"id": x["payload"]["id"], "first_name": x["payload"]["first_name"]})
            .distinct()
        )
        #
        root_topologyNode = (
            click_topologyNode
            .join(customer_topologyNode,
                  left_on_function=lambda l: l["user_id"],
                  right_on_function=lambda r: r["id"],
                  projection_function=lambda l, r: {
                      "user_id": l["user_id"],
                      "ip": l["ip"],
                      "first_name": r["first_name"]})
        )
        #
        runner.root(root_topologyNode)
        #
        return runner

    def get_runner_2_joins(self, click_source_str, customer_source_str, product_source_str):
        runner = Runner()
        #
        source_click_topologyNode = runner.source(click_source_str)
        source_customer_topologyNode = runner.source(customer_source_str)
        source_product_topologyNode = runner.source(product_source_str)
        #
        click_topologyNode = (
            source_click_topologyNode
            .map(lambda x: {"user_id": x["payload"]["user_id"], "ip": x["payload"]["ip"], "product_id": x["payload"]["product_id"]})
            .distinct()
        )
        #
        customer_topologyNode = (
            source_customer_topologyNode
            .map(lambda x: {"id": x["payload"]["id"], "first_name": x["payload"]["first_name"]})
            .distinct()
        )
        #
        product_topologyNode = (
            source_product_topologyNode
            .map(lambda x: {"id": x["payload"]["id"], "brand": x["payload"]["brand"]})
            .distinct()
        )
        #
        root_topologyNode = (
            click_topologyNode
            .join(customer_topologyNode,
                  left_on_function=lambda l: l["user_id"],
                  right_on_function=lambda r: r["id"],
                  projection_function=lambda l, r: {
                      "user_id": l["user_id"],
                      "ip": l["ip"],
                      "product_id": l["product_id"],
                      "first_name": r["first_name"]})
                      .join(product_topologyNode,
                            left_on_function=lambda l: l["product_id"],
                            right_on_function=lambda r: r["id"],
                            projection_function=lambda l, r: {"user_id": l["user_id"],
                                                              "ip": l["ip"],
                                                              "product_id": l["product_id"],
                                                              "first_name": l["first_name"],
                                                              "brand": r["brand"]})
        )
        #
        runner.root(root_topologyNode)
        #
        return runner

    def get_runner_3_joins(self, click_source_str, customer_source_str, product_source_str, order_source_str):
        runner = Runner()
        #
        source_click_topologyNode = runner.source(click_source_str)
        source_customer_topologyNode = runner.source(customer_source_str)
        source_product_topologyNode = runner.source(product_source_str)
        source_order_topologyNode = runner.source(order_source_str)
        #
        click_topologyNode = (
            source_click_topologyNode
            .map(lambda x: {"user_id": x["payload"]["user_id"], "ip": x["payload"]["ip"], "product_id": x["payload"]["product_id"]})
            .distinct()
        )
        #
        customer_topologyNode = (
            source_customer_topologyNode
            .map(lambda x: {"id": x["payload"]["id"], "first_name": x["payload"]["first_name"]})
            .distinct()
        )
        #
        product_topologyNode = (
            source_product_topologyNode
            .map(lambda x: {"id": x["payload"]["id"], "brand": x["payload"]["brand"]})
            .distinct()
        )
        #
        order_topologyNode = (
            source_order_topologyNode
            .map(lambda x: {"order_id": x["payload"]["order_id"], "product_id": x["payload"]["product_id"], "customer_id": x["payload"]["customer_id"]})
            .distinct()
        )
        #
        root_topologyNode = (
            click_topologyNode.join(
                order_topologyNode,
                left_on_function=lambda l: {"product_id": l["product_id"], "user_id": l["user_id"]},
                right_on_function=lambda r: {"product_id": r["product_id"], "user_id": r["customer_id"]},
                projection_function=lambda l, r: {
                    "user_id": l["user_id"],
                    "ip": l["ip"],
                    "product_id": l["product_id"],
                    "order_id": r["order_id"]})
                    .join(
                        customer_topologyNode,
                        left_on_function=lambda l: l["user_id"],
                        right_on_function=lambda r: r["id"],
                        projection_function=lambda l, r: {
                            "user_id": l["user_id"],
                            "ip": l["ip"],
                            "product_id": l["product_id"],
                            "order_id": l["order_id"],
                            "first_name": r["first_name"]})
                            .join(
                                product_topologyNode,
                                left_on_function=lambda l: l["product_id"],
                                right_on_function=lambda r: r["id"],
                                projection_function=lambda l, r: {
                                    "user_id": l["user_id"],
                                    "ip": l["ip"],
                                    "product_id": l["product_id"],
                                    "first_name": l["first_name"],
                                    "brand": r["brand"],
                                    "order_id": l["order_id"]})
        )
        #
        runner.root(root_topologyNode)
        #
        return runner

    #

    def generate(self, source_str, batch_size_int):
        message_dict_list = []
        #
        if source_str == "shoe_clickstream":
            generator = ShoeClickstreamGenerator()
        elif source_str == "shoe_customers":
            generator = ShoeCustomerGenerator()
        elif source_str == "shoes":
            generator = ShoeProductGenerator()
        elif source_str == "shoe_orders":
            generator = ShoeOrderGenerator()
        else:
            raise Exception(f"Only shoe_clickstream, shoe_customers, shoes, shoe_orders supported: {source_str}")
        #
        for _ in range(batch_size_int):
            record_dict = generator.generate_record()
            message_dict = {"key": None,
                            "value": {"payload": record_dict}}
            message_dict_list.append(message_dict)
        #
        return message_dict_list 
