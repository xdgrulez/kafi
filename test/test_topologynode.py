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

#

table_str_zset_dict = {}

#

class TestTopologyNode(unittest.TestCase):
    def setUp(self):
        #
        df = pd.read_csv("test/sqlzoo/world.csv")
        world_message_dict_list = []
        for _, row_series in df.iterrows():
            value_dict = row_series.to_dict()
            message_dict = {"key": value_dict["name"],
                            "value": value_dict}
            world_message_dict_list.append(message_dict)
        #
        world_zset = message_dict_list_to_ZSet(world_message_dict_list)
        table_str_zset_dict["world"] = world_zset
        #
        print("Test:", self._testMethodName)

    def tearDown(self):
        pass

    #

    def test_sqlzoo_select_basics_1(self):
        world_source_topologyNode = source("world")
        #
        limit_topologyNode = (
            world_source_topologyNode
            .limit(100)
        )
        #
        world_zset = table_str_zset_dict["world"]
        world_source_topologyNode.output_handle_function().get().send(world_zset)
        #
        limit_topologyNode.step()
        limit_topologyNode.gc()
        #
        latest_zset = limit_topologyNode.latest()
        #
        self.assertEqual(100, len(latest_zset.inner))
        #
        value_dict = json.loads(list(latest_zset.inner.keys())[78])
        self.assertEqual("Iran", value_dict["name"])
        self.assertEqual("Asia", value_dict["continent"])
        self.assertEqual(1648195, value_dict["area"])
        self.assertEqual(83396540, value_dict["population"])
        self.assertEqual(460976000000, value_dict["gdp"])
        self.assertEqual("Tehran", value_dict["capital"])
        self.assertEqual(".ir", value_dict["tld"])
        self.assertEqual("//upload.wikimedia.org/wikipedia/commons/c/ca/Flag_of_Iran.svg", value_dict["flag"])
