import datetime
import json
import os
import sys
import unittest

import pandas as pd

#

if os.path.basename(os.getcwd()) == "test":
    sys.path.insert(1, "..")
else:
    sys.path.insert(1, ".")

from kafi.streams.topologynode import get, update, sum, agg_tuple, message_dict_list_to_ZSet, source
from kafi.kafi import *

#

table_str_zset_dict = {}

#

class TestTopologyNodeDatagen(unittest.TestCase):
    def setUp(self):
        #
        print("Test:", self._testMethodName)

    def tearDown(self):
        pass

    #

    def test_1_join(self):
        click_topic_str = "shoe_clickstream"
        customer_topic_str = "shoe_customers"
        #
        source_click_topologyNode = source(click_topic_str)
        source_customer_topologyNode = source(customer_topic_str)
        #
        click_topologyNode = (
            source_click_topologyNode
            .map(lambda x: {"user_id": x["user_id"], "ip": x["ip"]})
        )
        #
        customer_topologyNode = (
            source_customer_topologyNode
            .map(lambda x: {"id": x["id"], "first_name": x["first_name"]})
        )
        #
        root_topologyNode = (
            click_topologyNode
            .join(customer_topologyNode,
                  on_function=lambda l, r: l["user_id"] == r["id"],
                  projection_function=lambda l, r: {"user_id": l["user_id"],
                                                    "ip": l["ip"],
                                                    "first_name": r["first_name"]},
                  profile_config_dict = None)
                #   profile_config_dict = {"gc": {"memory": {"after": True, "delta": True}, "streams": "size"}, "include": []})
        )
        #
        cluster = Cluster("local")
        cluster.consume_batch_size(1000)
        click_consumer = cluster.consumer(click_topic_str, value_type="avro")
        customer_consumer = cluster.consumer(customer_topic_str, value_type="avro")
        clicks_int = cluster.l(click_topic_str)[click_topic_str]
        customers_int = cluster.l(customer_topic_str)[customer_topic_str]
        n = 0
        start_time_int = time.time()
        while True:
            click_message_dict_list = click_consumer.consume()
            customer_message_dict_list = customer_consumer.consume()
            #
            click_int = click_message_dict_list[-1]["offset"] if len(click_message_dict_list) > 0 else clicks_int - 1
            customer_int = customer_message_dict_list[-1]["offset"] if len(customer_message_dict_list) > 0 else customers_int - 1
            #
            click_zset = message_dict_list_to_ZSet(click_message_dict_list)
            customer_zset = message_dict_list_to_ZSet(customer_message_dict_list)
            #
            source_click_topologyNode.output_handle_function().get().send(click_zset)
            source_customer_topologyNode.output_handle_function().get().send(customer_zset)
            #
            root_topologyNode.step()
            root_topologyNode.gc()
            #
            latest_zset = root_topologyNode.latest()
            #
            len_latest_zset = len(latest_zset.inner.keys())
            print(f"{len(click_message_dict_list)}/{len(customer_message_dict_list)}/{len_latest_zset} --- {click_int + 1}/{clicks_int}; {customer_int + 1}/{customers_int}")
            n += len_latest_zset
            #
            if click_int == clicks_int - 1 and customer_int == customers_int - 1:
                break
        #
        end_time_int = time.time()
        #
        click_consumer.close()
        customer_consumer.close()
        #
        print(f"Would have produced {n} outputs in {end_time_int - start_time_int}")

    def test_2_joins(self):
        click_topic_str = "shoe_clickstream"
        customer_topic_str = "shoe_customers"
        product_topic_str = "shoes"
        #
        source_click_topologyNode = source(click_topic_str)
        source_customer_topologyNode = source(customer_topic_str)
        source_product_topologyNode = source(product_topic_str)
        #
        clickstream_topologyNode = (
            source_click_topologyNode
            .map(lambda x: {"user_id": x["user_id"], "ip": x["ip"], "product_id": x["product_id"]})
        )
        #
        customers_topologyNode = (
            source_customer_topologyNode
            .map(lambda x: {"id": x["id"], "first_name": x["first_name"]})
        )
        #
        product_topologyNode = (
            source_product_topologyNode
            .map(lambda x: {"id": x["id"], "brand": x["brand"]})
        )
        #
        root_topologyNode = (
            clickstream_topologyNode
            .join(customers_topologyNode,
                  on_function=lambda l, r: l["user_id"] == r["id"],
                  projection_function=lambda l, r: {"user_id": l["user_id"],
                                                    "ip": l["ip"],
                                                    "product_id": l["product_id"],
                                                    "first_name": r["first_name"]})
                  .join(product_topologyNode,
                        on_function=lambda l, r: l["product_id"] == r["id"],
                        projection_function=lambda l, r: {"user_id": l["user_id"],
                                                          "ip": l["ip"],
                                                          "product_id": l["product_id"],
                                                          "first_name": l["first_name"],
                                                          "brand": r["brand"]},
                        profile_config_dict = None)
                        # profile_config_dict = {"gc": {"memory": {"after": True, "delta": True}, "streams": "size"}, "include": []})
        )
        #
        cluster = Cluster("local")
        cluster.consume_batch_size(1000)
        click_consumer = cluster.consumer(click_topic_str, value_type="avro")
        customer_consumer = cluster.consumer(customer_topic_str, value_type="avro")
        product_consumer = cluster.consumer(product_topic_str, value_type="avro")
        clicks_int = cluster.l(click_topic_str)[click_topic_str]
        customers_int = cluster.l(customer_topic_str)[customer_topic_str]
        products_int = cluster.l(product_topic_str)[product_topic_str]
        n = 0
        start_time_int = time.time()
        while True:
            click_message_dict_list = click_consumer.consume()
            customer_message_dict_list = customer_consumer.consume()
            product_message_dict_list = product_consumer.consume()
            #
            click_int = click_message_dict_list[-1]["offset"] if len(click_message_dict_list) > 0 else clicks_int - 1
            customer_int = customer_message_dict_list[-1]["offset"] if len(customer_message_dict_list) > 0 else customers_int - 1
            product_int = product_message_dict_list[-1]["offset"] if len(product_message_dict_list) > 0 else products_int - 1
            #
            click_zset = message_dict_list_to_ZSet(click_message_dict_list)
            customer_zset = message_dict_list_to_ZSet(customer_message_dict_list)
            product_zset = message_dict_list_to_ZSet(product_message_dict_list)
            #
            source_click_topologyNode.output_handle_function().get().send(click_zset)
            source_customer_topologyNode.output_handle_function().get().send(customer_zset)
            source_product_topologyNode.output_handle_function().get().send(product_zset)
            #
            root_topologyNode.step()
            root_topologyNode.gc()
            #
            latest_zset = root_topologyNode.latest()
            print(latest_zset)
            #
            len_latest_zset = len(latest_zset.inner.keys())
            print(f"{len(click_message_dict_list)}/{len(customer_message_dict_list)}/{len_latest_zset} --- {click_int + 1}/{clicks_int}; {customer_int + 1}/{customers_int}; {product_int + 1}/{products_int}")
            n += len_latest_zset
            #
            if click_int == clicks_int - 1 and customer_int == customers_int - 1 and product_int == products_int - 1:
                break
        #
        end_time_int = time.time()
        #
        click_consumer.close()
        customer_consumer.close()
        product_consumer.close()
        #
        print(f"Would have produced {n} outputs in {end_time_int - start_time_int}")

    def test_3_joins(self):
        click_topic_str = "shoe_clickstream"
        customer_topic_str = "shoe_customers"
        product_topic_str = "shoes"
        order_topic_str = "shoe_orders"
        #
        source_click_topologyNode = source(click_topic_str)
        source_customer_topologyNode = source(customer_topic_str)
        source_product_topologyNode = source(product_topic_str)
        source_order_topologyNode = source(order_topic_str)
        #
        click_topologyNode = (
            source_click_topologyNode
            .map(lambda x: {"user_id": x["user_id"], "ip": x["ip"], "product_id": x["product_id"]})
        )
        #
        customer_topologyNode = (
            source_customer_topologyNode
            .map(lambda x: {"id": x["id"], "first_name": x["first_name"]})
        )
        #
        product_topologyNode = (
            source_product_topologyNode
            .map(lambda x: {"id": x["id"], "brand": x["brand"]})
        )
        #
        order_topologyNode = (
            source_order_topologyNode
            .map(lambda x: {"order_id": x["order_id"], "product_id": x["product_id"], "customer_id": x["customer_id"]})
        )
        #
        root_topologyNode = (
            click_topologyNode.join(
                order_topologyNode,
                on_function=lambda l, r: l["product_id"] == r["product_id"]
                and l["user_id"] == r["customer_id"],
                projection_function=lambda l, r: {
                    "user_id": l["user_id"],
                    "ip": l["ip"],
                    "product_id": l["product_id"],
                    "order_id": r["order_id"]
                },
                # profile_config_dict={
                #     "gc": {"memory": {"after": True, "delta": True}, "streams": "size"},
                #     "include": []
                # }
                profile_config_dict=None
            )
            .join(
                customer_topologyNode,
                on_function=lambda l, r: l["user_id"] == r["id"],
                projection_function=lambda l, r: {
                    "user_id": l["user_id"],
                    "ip": l["ip"],
                    "product_id": l["product_id"],
                    "order_id": l["order_id"],
                    "first_name": r["first_name"]
                },
                # profile_config_dict={
                #     "gc": {"memory": {"after": True, "delta": True}, "streams": "size"},
                #     "include": []
                # }
                profile_config_dict=None
            )
            .join(
                product_topologyNode,
                on_function=lambda l, r: l["product_id"] == r["id"],
                projection_function=lambda l, r: {
                    "user_id": l["user_id"],
                    "ip": l["ip"],
                    "product_id": l["product_id"],
                    "first_name": l["first_name"],
                    "brand": r["brand"]
                },
                # profile_config_dict={
                #     "gc": {"memory": {"after": True, "delta": True}, "streams": "size"},
                #     "include": []
                # }
                profile_config_dict=None
            )
        )
        #
        cluster = Cluster("local")
        cluster.consume_batch_size(100)
        click_consumer = cluster.consumer(click_topic_str, value_type="avro")
        customer_consumer = cluster.consumer(customer_topic_str, value_type="avro")
        product_consumer = cluster.consumer(product_topic_str, value_type="avro")
        order_consumer = cluster.consumer(order_topic_str, value_type="avro")
        clicks_int = cluster.l(click_topic_str)[click_topic_str]
        customers_int = cluster.l(customer_topic_str)[customer_topic_str]
        products_int = cluster.l(product_topic_str)[product_topic_str]
        orders_int = cluster.l(order_topic_str)[order_topic_str]
        n = 0
        start_time_int = time.time()
        while True:
            click_message_dict_list = click_consumer.consume()
            customer_message_dict_list = customer_consumer.consume()
            product_message_dict_list = product_consumer.consume()
            order_message_dict_list = order_consumer.consume()
            #
            click_int = click_message_dict_list[-1]["offset"] if len(click_message_dict_list) > 0 else clicks_int - 1
            customer_int = customer_message_dict_list[-1]["offset"] if len(customer_message_dict_list) > 0 else customers_int - 1
            product_int = product_message_dict_list[-1]["offset"] if len(product_message_dict_list) > 0 else products_int - 1
            order_int = order_message_dict_list[-1]["offset"] if len(order_message_dict_list) > 0 else orders_int - 1
            #
            click_zset = message_dict_list_to_ZSet(click_message_dict_list)
            customer_zset = message_dict_list_to_ZSet(customer_message_dict_list)
            product_zset = message_dict_list_to_ZSet(product_message_dict_list)
            order_zset = message_dict_list_to_ZSet(order_message_dict_list)
            #
            source_click_topologyNode.output_handle_function().get().send(click_zset)
            source_customer_topologyNode.output_handle_function().get().send(customer_zset)
            source_product_topologyNode.output_handle_function().get().send(product_zset)
            source_order_topologyNode.output_handle_function().get().send(order_zset)
            #
            root_topologyNode.step()
            root_topologyNode.gc()
            #
            latest_zset = root_topologyNode.latest()
            # print(latest_zset)
            #
            len_latest_zset = len(latest_zset.inner.keys())
            print(f"{len(click_message_dict_list)}/{len(customer_message_dict_list)}/{len_latest_zset} --- {click_int + 1}/{clicks_int}; {customer_int + 1}/{customers_int}; {product_int + 1}/{products_int}; {order_int + 1}/{orders_int}")
            n += len_latest_zset
            #
            if click_int == clicks_int - 1 and customer_int == customers_int - 1 and product_int == products_int - 1 and order_int == orders_int - 1:
                break
        #
        end_time_int = time.time()
        #
        click_consumer.close()
        customer_consumer.close()
        product_consumer.close()
        order_consumer.close()
        #
        print(f"Would have produced {n} outputs in {end_time_int - start_time_int}")
