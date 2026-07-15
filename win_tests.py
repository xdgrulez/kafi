import sys
import unittest
import importlib

sys.path.insert(1, "..")

import kafi.streams.topologynode
importlib.reload(kafi.streams.topologynode)

from kafi.streams.topologynode import TopologyNode as Tn
from test.streams.datagen.shoe_orders import ts_step_int
from streams.test_topologynode_base import TestTopologyNodeBase
from streams.test_generate import TestGenerate
from streams.test_base import TestBase

# Hilfsfunktionen für die Aggregation
def agg_function(agg, record, weight):
    return {
        "orders": agg["orders"] + weight,
        "total_price": agg["total_price"] + record["price"] * weight
    }

def projection_function(by, agg):
    return {
        "customer_id": by,
        "orders": agg["orders"],
        "total_price": agg["total_price"]
    }

--

def test_tumbling_window_split(self):
        customer_source_str = "shoe_customers"
        order_source_str = "shoe_orders"
        sink_str = "tumbling_output"
        
        size_int = ts_step_int * 10
        allowed_lateness_int = 1 * size_int

        # 1. Quellen oben separat mit retention begrenzen
        active_customers = (
            Tn.source(customer_source_str).from_value()
            .tumbling_retention(lambda x: x["ts"], size_int, allowed_lateness_int)
        )
        active_orders = (
            Tn.source(order_source_str).from_value()
            .tumbling_retention(lambda x: x["ts"], size_int, allowed_lateness_int)
        )

        # 2. Join auf den bereinigten Daten
        joined = active_orders.join_equi(
            active_customers,
            left_select_function=lambda order: order["customer_id"],
            right_select_function=lambda customer: customer["id"],
            projection_function=lambda order, customer: {
                "customer_id": customer["id"],
                "price": order["price"],
                "ts": order["ts"]
            }
        )

        # 3. Fenster aggregieren (ohne erneute Retention)
        window_tn = joined.tumbling(
            size_int,
            lambda x: x["ts"],
            lambda x: x["customer_id"],
            agg_function,
            {"orders": 0, "total_price": 0},
            projection_function
        )

        built_tn = Tn.build(window_tn.sink(sink_str))
        self.process(built_tn, {customer_source_str: 10, order_source_str: 10}, steps_int=15)
        self.assertTrue(len(self.sink_str_updated_record_any_list_dict[sink_str]) > 0)

def test_hopping_window_split(self):
        customer_source_str = "shoe_customers"
        order_source_str = "shoe_orders"
        sink_str = "hopping_output"
        
        size_int = ts_step_int * 10
        advance_int = ts_step_int * 5
        allowed_lateness_int = 1 * size_int

        active_customers = (
            Tn.source(customer_source_str).from_value()
            .hopping_retention(lambda x: x["ts"], size_int, advance_int, allowed_lateness_int)
        )
        active_orders = (
            Tn.source(order_source_str).from_value()
            .hopping_retention(lambda x: x["ts"], size_int, advance_int, allowed_lateness_int)
        )

        joined = active_orders.join_equi(
            active_customers,
            left_select_function=lambda order: order["customer_id"],
            right_select_function=lambda customer: customer["id"],
            projection_function=lambda order, customer: {
                "customer_id": customer["id"],
                "price": order["price"],
                "ts": order["ts"]
            }
        )

        window_tn = joined.hopping(
            size_int,
            advance_int,
            lambda x: x["ts"],
            lambda x: x["customer_id"],
            agg_function,
            {"orders": 0, "total_price": 0},
            projection_function
        )

        built_tn = Tn.build(window_tn.sink(sink_str))
        self.process(built_tn, {customer_source_str: 10, order_source_str: 10}, steps_int=15)
        self.assertTrue(len(self.sink_str_updated_record_any_list_dict[sink_str]) > 0)

def test_cumulative_window_split(self):
        customer_source_str = "shoe_customers"
        order_source_str = "shoe_orders"
        sink_str = "cumulative_output"
        
        size_int = ts_step_int * 15
        advance_int = ts_step_int * 5
        allowed_lateness_int = 1 * size_int

        active_customers = (
            Tn.source(customer_source_str).from_value()
            .cumulative_retention(lambda x: x["ts"], size_int, advance_int, allowed_lateness_int)
        )
        active_orders = (
            Tn.source(order_source_str).from_value()
            .cumulative_retention(lambda x: x["ts"], size_int, advance_int, allowed_lateness_int)
        )

        joined = active_orders.join_equi(
            active_customers,
            left_select_function=lambda order: order["customer_id"],
            right_select_function=lambda customer: customer["id"],
            projection_function=lambda order, customer: {
                "customer_id": customer["id"],
                "price": order["price"],
                "ts": order["ts"]
            }
        )

        window_tn = joined.cumulative(
            size_int,
            advance_int,
            lambda x: x["ts"],
            lambda x: x["customer_id"],
            agg_function,
            {"orders": 0, "total_price": 0},
            projection_function
        )

        built_tn = Tn.build(window_tn.sink(sink_str))
        self.process(built_tn, {customer_source_str: 10, order_source_str: 10}, steps_int=15)
        self.assertTrue(len(self.sink_str_updated_record_any_list_dict[sink_str]) > 0)

    def test_session_window_split(self):
        customer_source_str = "shoe_customers"
        order_source_str = "shoe_orders"
        sink_str = "session_output"
        
        gap_int = ts_step_int * 5
        max_session_int = ts_step_int * 20
        allowed_lateness_int = 1 * max_session_int

        active_customers = (
            Tn.source(customer_source_str).from_value()
            .session_retention(lambda x: x["ts"], max_session_int, allowed_lateness_int)
        )
        active_orders = (
            Tn.source(order_source_str).from_value()
            .session_retention(lambda x: x["ts"], max_session_int, allowed_lateness_int)
        )

        joined = active_orders.join_equi(
            active_customers,
            left_select_function=lambda order: order["customer_id"],
            right_select_function=lambda customer: customer["id"],
            projection_function=lambda order, customer: {
                "customer_id": customer["id"],
                "price": order["price"],
                "ts": order["ts"]
            }
        )

        window_tn = joined.session(
            gap_int,
            max_session_int,
            lambda x: x["ts"],
            lambda x: x["customer_id"],
            agg_function,
            {"orders": 0, "total_price": 0},
            projection_function
        )

        built_tn = Tn.build(window_tn.sink(sink_str))
        self.process(built_tn, {customer_source_str: 10, order_source_str: 10}, steps_int=15)
        self.assertTrue(len(self.sink_str_updated_record_any_list_dict[sink_str]) > 0)


        def test_sliding_window_split(self):
        customer_source_str = "shoe_customers"
        order_source_str = "shoe_orders"
        sink_str = "sliding_output"
        
        size_int = ts_step_int * 10
        allowed_lateness_int = 1 * size_int

        active_customers = (
            Tn.source(customer_source_str).from_value()
            .sliding_retention(lambda x: x["ts"], size_int, allowed_lateness_int)
        )
        active_orders = (
            Tn.source(order_source_str).from_value()
            .sliding_retention(lambda x: x["ts"], size_int, allowed_lateness_int)
        )

        joined = active_orders.join_equi(
            active_customers,
            left_select_function=lambda order: order["customer_id"],
            right_select_function=lambda customer: customer["id"],
            projection_function=lambda order, customer: {
                "customer_id": customer["id"],
                "price": order["price"],
                "ts": order["ts"]
            }
        )

        # Sliding triggert dank deines Entwurfs vor dem group_by!
        window_tn = joined.sliding(
            size_int,
            lambda x: x["ts"],
            lambda x: x["customer_id"],
            agg_function,
            {"orders": 0, "total_price": 0},
            projection_function
        )

        built_tn = Tn.build(window_tn.sink(sink_str))
        self.process(built_tn, {customer_source_str: 10, order_source_str: 10}, steps_int=15)
        self.assertTrue(len(self.sink_str_updated_record_any_list_dict[sink_str]) > 0)
