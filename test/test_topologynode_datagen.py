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

    def test_join(self):
        source_clickstream_topologyNode = source("clickstream")
        source_customers_topologyNode = source("customers")
        #
        root_topologyNode = (
            source_clickstream_topologyNode
            .join(source_customers_topologyNode,
                  on_function=lambda l, r: l["user_id"] == r["id"],
                  projection_function=lambda l, r: {"user_id": l["user_id"],
                                                    "ip": l["ip"],
                                                    "first_name": r["first_name"]})
        )
        #
        cluster = Cluster("local")
        clickstream_consumer = cluster.consumer("shoe_clickstream", value_type="avro")
        customers_consumer = cluster.consumer("shoe_customers", value_type="avro")
        while True:
            clickstream_message_dict_list = clickstream_consumer.consume()
            customers_message_dict_list = customers_consumer.consume()
            #
            clickstream_zset = message_dict_list_to_ZSet(clickstream_message_dict_list)
            customers_zset = message_dict_list_to_ZSet(customers_message_dict_list)
            #
            source_clickstream_topologyNode.output_handle_function().get().send(clickstream_zset)
            source_customers_topologyNode.output_handle_function().get().send(customers_zset)
            #
            root_topologyNode.step()
            root_topologyNode.gc()
            #
            latest_zset = root_topologyNode.latest()
            #
            print(latest_zset)
            #
            if len(clickstream_message_dict_list) == 0 and len(customers_message_dict_list) == 0:
                break
        #
        clickstream_consumer.close()
        customers_consumer.close()
