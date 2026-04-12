# Would have produced 1 outputs in 52.037787675857544 (join2)
# Flink: result topic has 7583583 mostly wrong messages ;)

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

class TestTopologyNodeJamie(unittest.TestCase):
    def setUp(self):
        #
        print("Test:", self._testMethodName)

    def tearDown(self):
        pass

    #

    def test_jamie(self):
        transaction_topic_str = "transactions"
        #
        source_transaction_topologyNode = source(transaction_topic_str)
        #
        transaction_topologyNode = (
             source_transaction_topologyNode.map(lambda x: {"from_account": x["from_account"],
                                                            "to_account": x["to_account"],
                                                            "amount": x["amount"]})
        )
        #
        credits_topologyNode = (
            transaction_topologyNode
            .group_by_agg(by_function_list=[get("to_account")],
                        as_function=update("account"),
                        agg_tuple_list=[agg_tuple(select_function=get("amount"),
                                                    agg_function=sum,
                                                    as_function=update("credits"))],
                        profile_config_dict=None)
        )
        #
        debits_topologyNode = (
            transaction_topologyNode
            .group_by_agg([get("from_account")],
                        update("account"),
                        [agg_tuple(get("amount"), sum, update("debits"))],
                        profile_config_dict=None)
        )
        #
        balance_topologyNode = (
            credits_topologyNode
            .join2(
                debits_topologyNode,
                left_on_function=lambda l: l["account"],
                right_on_function=lambda r: r["account"],
                projection_function=lambda l, r: {"account": l["account"],
                                                "balance": l["credits"] - r["debits"]},
                profile_config_dict = None)
        )
        #
        root_topologyNode = (
            balance_topologyNode
            .agg(agg_tuple_list=[agg_tuple(select_function=get("balance"), 
                                        agg_function=sum,
                                        as_function=update("sum"))],
                 profile_config_dict=None)
        )
        #
        cluster = Cluster("local")
        cluster.consume_batch_size(1000)
        transaction_consumer = cluster.consumer(transaction_topic_str)
        transactions_int = cluster.l(transaction_topic_str)[transaction_topic_str]
        n = 0
        start_time_int = time.time()
        while True:
            transaction_message_dict_list = transaction_consumer.consume()
            #
            transaction_int = transaction_message_dict_list[-1]["offset"] if len(transaction_message_dict_list) > 0 else transactions_int - 1
            #
            transaction_zset = message_dict_list_to_ZSet(transaction_message_dict_list)
            #
            source_transaction_topologyNode.output_handle_function().get().send(transaction_zset)
            #
            root_topologyNode.step()
            root_topologyNode.gc()
            #
            latest_zset = root_topologyNode.latest()
            print(latest_zset)
            #
            len_latest_zset = len(latest_zset.inner.keys())
            print(f"{len(transaction_message_dict_list)}/{len_latest_zset} --- {transaction_int + 1}/{transactions_int}")
            n += len_latest_zset
            #
            if transaction_int >= transactions_int - 1:
                break
        #
        end_time_int = time.time()
        #
        transaction_consumer.close()
        #
        print(f"Would have produced {n} outputs in {end_time_int - start_time_int}")
