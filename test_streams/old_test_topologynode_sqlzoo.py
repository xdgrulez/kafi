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

    def get_latest(self, root_topologyNode, source_topologyNode):
        table_str = source_topologyNode.name()
        zset = table_str_zset_dict[table_str]
        source_topologyNode.output_handle_function().get().send(zset)
        #
        root_topologyNode.step()
        root_topologyNode.gc()
        #
        latest_zset = root_topologyNode.latest()
        #
        value_dict_list = [json.loads(value_str) for value_str in list(latest_zset.inner.keys())]
        #
        return value_dict_list

    #

    def test_sqlzoo_limit(self):
        world_source_topologyNode = source("world")
        #
        root_topologyNode = (
            world_source_topologyNode
            .limit(100)
        )
        #
        value_dict_list = self.get_latest(root_topologyNode, world_source_topologyNode)
        #
        self.assertEqual(100, len(value_dict_list))
        #
        value_dict = value_dict_list[78]
        self.assertEqual("Iran", value_dict["name"])
        self.assertEqual("Asia", value_dict["continent"])
        self.assertEqual(1648195, value_dict["area"])
        self.assertEqual(83396540, value_dict["population"])
        self.assertEqual(460976000000, value_dict["gdp"])
        self.assertEqual("Tehran", value_dict["capital"])
        self.assertEqual(".ir", value_dict["tld"])
        self.assertEqual("//upload.wikimedia.org/wikipedia/commons/c/ca/Flag_of_Iran.svg", value_dict["flag"])

# SELECT population FROM world
#   WHERE name = 'Germany'
    def test_sqlzoo_select_basics_1(self):
        world_source_topologyNode = source("world")
        #
        root_topologyNode = (
            world_source_topologyNode
            .filter(lambda value_dict: value_dict["name"] == "Germany")
            .map(lambda value_dict: {"population": value_dict["population"]})
        )
        #
        value_dict_list = self.get_latest(root_topologyNode, world_source_topologyNode)
        #
        self.assertEqual(1, len(value_dict_list))
        #
        value_dict = value_dict_list[0]
        self.assertEqual({"population": 83149300}, value_dict)

# SELECT name, population FROM world
#   WHERE name IN ('Sweden', 'Norway', 'Denmark');
    def test_sqlzoo_select_basics_2(self):
        world_source_topologyNode = source("world")
        #
        root_topologyNode = (
            world_source_topologyNode
            .filter(lambda value_dict: value_dict["name"] in ["Sweden", "Norway", "Denmark"])
            .map(lambda value_dict: {"name": value_dict["name"],
                                     "population": value_dict["population"]})
        )
        #
        value_dict_list = self.get_latest(root_topologyNode, world_source_topologyNode)
        #
        self.assertEqual(3, len(value_dict_list))
        #
        self.assertEqual({"name": "Denmark", "population": 5822763}, value_dict_list[0])
        self.assertEqual({"name": "Norway", "population": 5367580}, value_dict_list[1])
        self.assertEqual({"name": "Sweden", "population": 10338368}, value_dict_list[2])

# SELECT name, area FROM world
#   WHERE area BETWEEN 200000 AND 250000    def test_sqlzoo_select_basics_3(self):
    def test_sqlzoo_select_basics_3(self):
        world_source_topologyNode = source("world")
        #
        root_topologyNode = (
            world_source_topologyNode
            .filter(lambda value_dict: value_dict["area"] >= 200000 and value_dict["area"] <= 250000)
            .map(lambda value_dict: {"name": value_dict["name"],
                                     "area": value_dict["area"]})
        )
        #
        value_dict_list = self.get_latest(root_topologyNode, world_source_topologyNode)
        #
        self.assertEqual(8, len(value_dict_list))
        #
        self.assertEqual({"name": "Belarus", "area": 207600}, value_dict_list[0])
        self.assertEqual({"name": "Ghana", "area": 238533}, value_dict_list[1])
        self.assertEqual({"name": "Guinea", "area": 245857}, value_dict_list[2])
        self.assertEqual({"name": "Guyana", "area": 214969}, value_dict_list[3])
        self.assertEqual({"name": "Laos", "area": 236800}, value_dict_list[4])
        self.assertEqual({"name": "Romania", "area": 238391}, value_dict_list[5])
        self.assertEqual({"name": "Uganda", "area": 241550}, value_dict_list[6])
        self.assertEqual({"name": "United Kingdom", "area": 242900}, value_dict_list[7])
