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
        clickstream_topologyNode = (
            source_clickstream_topologyNode
            .map(lambda x: {"user_id": x["user_id"], "ip": x["ip"]})
        )
        #
        customers_topologyNode = (
            source_customers_topologyNode
            .map(lambda x: {"id": x["id"], "first_name": x["first_name"]})
        )
        #
        root_topologyNode = (
            clickstream_topologyNode
            .join(customers_topologyNode,
                  on_function=lambda l, r: l["user_id"] == r["id"],
                  projection_function=lambda l, r: {"user_id": l["user_id"],
                                                    "ip": l["ip"],
                                                    "first_name": r["first_name"]},
                  profile_config_dict = {"gc": {"memory": {"after": True, "delta": True}, "streams": "size"}, "include": []}
)
        )
        #
        cluster = Cluster("local")
        cluster.consume_batch_size(1000)
        clickstream_consumer = cluster.consumer("shoe_clickstream", value_type="avro")
        customers_consumer = cluster.consumer("shoe_customers", value_type="avro")
        clickstreams_int = cluster.l("shoe_clickstream")["shoe_clickstream"]
        customers_int = cluster.l("shoe_customers")["shoe_customers"]
        n = 0
        start_time_int = time.time()
        while True:
            clickstream_message_dict_list = clickstream_consumer.consume()
            customers_message_dict_list = customers_consumer.consume()
            #
            clickstream_int = clickstream_message_dict_list[-1]["offset"] if len(clickstream_message_dict_list) > 0 else -1
            customer_int = customers_message_dict_list[-1]["offset"] if len(customers_message_dict_list) > 0 else -1
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
            len_latest_zset = len(latest_zset.inner.keys())
            print(f"{len(clickstream_message_dict_list)}/{len(customers_message_dict_list)}/{len_latest_zset} --- {clickstream_int}/{clickstreams_int}; {customer_int}/{customers_int}")
            n += len_latest_zset
            #
            if clickstream_int == clickstreams_int - 1 and customer_int == customers_int - 1:
                break
        #
        end_time_int = time.time()
        #
        clickstream_consumer.close()
        customers_consumer.close()
        #
        print(f"Would have produced {n} outputs in {end_time_int - start_time_int}")
