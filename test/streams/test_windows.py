import random, uuid, unittest

from kafi.streams.topologynode import TopologyNode as Tn

#

class TestWindows(unittest.TestCase):
    def setUp(self):
        self.order_source_str = "orders"
        self.sink_str = "sink"
        #
        self.order_generator = OrderGenerator()

    #

    def test_tumbling(self):
        size_int = 100
        allowed_lateness_int = size_int * 2
        #
        order_source_tn = Tn.source(self.order_source_str)
        order_source_tn.to_zSet(Tn._from_records)
        order_tn = (
            order_source_tn
            .map(lambda r: {"customer_id": r["value"]["customer_id"],
                            "price": r["value"]["price"],
                            "ts": r["value"]["ts"]})
            .expire_tumbling(lambda r: r["ts"], size_int, allowed_lateness_int)
            .distinct()
        )
        #
        sink_tn = order_tn.group_by_agg_tumbling(
            ts_fun=lambda r: r["ts"],
            size_int=size_int,
            key_fun=lambda r: r["customer_id"],
            agg_fun=lambda agg_r, r: {"orders": agg_r["orders"] + 1,
                                      "total_price": agg_r["total_price"] + r["price"],
                                      "last_ts": max(agg_r["last_ts"], r["ts"])},
            agg_initial_any={"orders": 0, "total_price": 0, "last_ts": 0},
            project_fun=lambda key_any, agg_r: {"customer_id": key_any,
                                                "orders": agg_r["orders"],
                                                "total_price": agg_r["total_price"],
                                                "last_ts": agg_r["last_ts"]},
            trigger_positive_only_bool=False
        ).sink(self.sink_str)
        #
        built_tn = Tn.build(sink_tn)
        built_tn.from_zSet(Tn._to_records)
        #
        print("=== Step 1: An order from customer 1 (price=100, ts=10) arrives ===")
        self.process(built_tn, customer_id=1, price=100, ts=10, w=1)
        print("-> OK")

        print("=== Step 1: An order from customer 1 (price=200, ts=50) arrives ===")
        self.process(built_tn, customer_id=1, price=200, ts=50, w=1)
        print("-> OK")

        print("\n=== Step 3: An order from customer 2 (price=50, ts=105) arrives ===")
        r_w_tuple_list = self.process(built_tn, customer_id=2, price=50, ts=105, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 2, "total_price": 300, "last_ts": 50, "window_end": 100}, 1)
        ])
        print("-> OK: Window [0, 100) triggered (orders=2, total=300).")

        print("\n=== Step 4: The order from customer 1 (price=200, ts=50) is retracted ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=200, ts=50, w=-1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 2, "total_price": 300, "last_ts": 50, "window_end": 100}, -1),
            ({"customer_id": 1, "orders": 1, "total_price": 100, "last_ts": 10, "window_end": 100}, 1)
        ])
        print("-> OK: Correction for window [0, 100) triggered: (customer=1, orders=1, total=100).")

        print("\n=== Step 5: An order from customer 3 (price=400, ts=101) arrives ===")
        r_w_tuple_list = self.process(built_tn, customer_id=3, price=400, ts=101, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK: Order put into window [100, 200), still correctly held back/not triggered.")

        print("\n=== Step 6: Another order from customer 3 at 150 (price=200, ts=150) arrives ===")
        r_w_tuple_list = self.process(built_tn, customer_id=3, price=200, ts=150, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK. Order put into window [100, 200), still correctly held back/not triggered.")

        print("\n=== Step 7: Another order from customer 1 (price=50, ts=210) arrives ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=50, ts=210, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 2, "orders": 1, "total_price": 50, "last_ts": 105, "window_end": 200}, 1),
            ({"customer_id": 3, "orders": 2, "total_price": 600, "last_ts": 150, "window_end": 200}, 1)
        ])
        print("-> OK: Window [100, 200) triggered (customer=2, orders=1, total=50), (customer=3, orders=2, total=600).")

        print("\n=== Step 8: An order from customer 2 (price=60, ts=330) arrives ===")
        r_w_tuple_list = self.process(built_tn, customer_id=2, price=60, ts=330, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 1, "total_price": 50, "last_ts": 210, "window_end": 300}, 1)
        ])
        print("-> OK: Window [200, 300) triggered (customer=1, orders=1, total=50).")

        print("\n=== Step 9: An order from customer 2 (price=40, ts=180) arrives late (but not too late) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=2, price=40, ts=180, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 2, "orders": 1, "total_price": 50, "last_ts": 105, "window_end": 200}, -1),
            ({"customer_id": 2, "orders": 2, "total_price": 90, "last_ts": 180, "window_end": 200}, 1)
        ])
        print("-> OK: Correction for window [100, 200) triggered: (customer=2, orders=2, total=90).")

        print("\n=== Step 10: An order from customer 1 (price=70, ts=510) arrives ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=70, ts=510, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 1, "total_price": 100, "last_ts": 10, "window_end": 100}, -1),
            ({"customer_id": 2, "orders": 2, "total_price": 90, "last_ts": 180, "window_end": 200}, -1),
            ({"customer_id": 3, "orders": 2, "total_price": 600, "last_ts": 150, "window_end": 200}, -1),
            ({"customer_id": 2, "orders": 1, "total_price": 60, "last_ts": 330, "window_end": 400}, 1)
        ])
        print("-> OK: Retraction for window [100, 200) triggered. Window (300, 400) triggered: (customer=2, orders=1, total=60).")

        print("\n=== Step 11: An order from customer 1 (price=70, ts=130) arrives too late ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=70, ts=130, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK: Order arrived too late - no action.")

    #

    def test_hopping(self):
        size_int = 100
        hop_int = size_int // 2
        allowed_lateness_int = size_int * 2
        #
        order_source_tn = Tn.source(self.order_source_str)
        order_source_tn.to_zSet(Tn._from_records)
        order_tn = (
            order_source_tn
            .map(lambda r: {"customer_id": r["value"]["customer_id"],
                            "price": r["value"]["price"],
                            "ts": r["value"]["ts"]})
            .expire_hopping(lambda r: r["ts"], size_int, hop_int, allowed_lateness_int)
            .distinct()
        )
        #
        sink_tn = order_tn.group_by_agg_hopping(
            ts_fun=lambda r: r["ts"],
            size_int=size_int,
            hop_int=hop_int,
            key_fun=lambda r: r["customer_id"],
            agg_fun=lambda agg_r, r: {"orders": agg_r["orders"] + 1,
                                    "total_price": agg_r["total_price"] + r["price"]},
            agg_initial_any={"orders": 0, "total_price": 0},
            project_fun=lambda key_any, agg_r: {"customer_id": key_any,
                                                "orders": agg_r["orders"],
                                                "total_price": agg_r["total_price"]},
            trigger_positive_only_bool=False
        ).sink(self.sink_str)                                       
        #
        built_tn = Tn.build(sink_tn)
        built_tn.from_zSet(Tn._to_records)        
        #
        print("=== Step 1: Two orders from customer 1 arrive (ts=10 falls into [0, 100); ts=60 falls into [0, 100) and [50, 150)) ===")
        self.process(built_tn, customer_id=1, price=100, ts=10, w=1)
        self.process(built_tn, customer_id=1, price=200, ts=60, w=1)
        print("-> OK.")

        print("\n=== Step 2: An order from customer 2 at ts=110 arrives ===")
        r_w_tuple_list = self.process(built_tn, customer_id=2, price=50, ts=110, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 2, "total_price": 300, "window_end": 100}, 1)
        ])
        print("-> OK: Window [0, 100) triggered: (customer=1, orders=2, total=300)")

        print("\n=== Step 3: Retraction for the window (50, 150) arrives ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=200, ts=60, w=-1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 2, "total_price": 300, "window_end": 100}, -1),
            ({"customer_id": 1, "orders": 1, "total_price": 100, "window_end": 100}, 1)
        ])
        print("-> OK: Correction for window [0, 100) triggered: (customer=1, orders=1, total=100).")

        print("\n=== Step 4: An order from customer 3 at arrives (price=99, ts=150) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=3, price=99, ts=150, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 2, "orders": 1, "total_price": 50, "window_end": 150}, 1)
        ])
        print("-> OK: Window [50, 150) triggered: (customer=2, orders=2, total=50).")

        print("\n=== Step 5: An order from customer 2 arrives (price=99, ts=250) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=2, price=90, ts=250, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 2, "orders": 1, "total_price": 50, "window_end": 200}, 1),
            ({"customer_id": 3, "orders": 1, "total_price": 99, "window_end": 200}, 1),
            ({"customer_id": 3, "orders": 1, "total_price": 99, "window_end": 250}, 1)
        ])
        print("-> OK: Windows [100, 200) and [150, 250) triggered.")

        print("\n=== Step 6: An order for customer 3 (price=100, ts=170) arrives late (but not too late) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=3, price=100, ts=170, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 3, "orders": 1, "total_price": 99, "window_end": 200}, -1),
            ({"customer_id": 3, "orders": 2, "total_price": 199, "window_end": 200}, 1),
            ({"customer_id": 3, "orders": 1, "total_price": 99, "window_end": 250}, -1),
            ({"customer_id": 3, "orders": 2, "total_price": 199, "window_end": 250}, 1)
        ])
        print("-> OK: Corrections for windows [100, 200) and [150, 250) triggered.")

        print("\n=== Step 7: An order for customer 1 (price=10, ts=450) arrives ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=10, ts=450, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 1, "total_price": 100, "window_end": 100}, -1),
            ({"customer_id": 2, "orders": 1, "total_price": 90, "window_end": 300}, 1),
            ({"customer_id": 2, "orders": 1, "total_price": 90, "window_end": 350}, 1)
        ])
        print("-> OK: Window [0, 100) correctly expired; windows [200, 300) and [250, 300) correctly triggered.")

        print("\n=== Step 8: An order from customer 1 (price=999, ts=20) arrives too late ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=999, ts=20, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK: Order arrived too late - no action.")

    #

    def test_cumulative(self):
        size_int = 100
        advance_int = size_int // 5
        allowed_lateness_int = size_int * 2
        #
        order_source_tn = Tn.source(self.order_source_str)
        order_source_tn.to_zSet(Tn._from_records)
        order_tn = (
            order_source_tn
            .map(lambda r: {"customer_id": r["value"]["customer_id"],
                            "price": r["value"]["price"],
                            "ts": r["value"]["ts"]})
            .expire_cumulative(lambda r: r["ts"], size_int, advance_int, allowed_lateness_int)
            .distinct()
        )
        #
        sink_tn = order_tn.group_by_agg_cumulative(
            ts_fun=lambda r: r["ts"],
            size_int=size_int,
            advance_int=advance_int,
            key_fun=lambda r: r["customer_id"],
            agg_fun=lambda agg_r, r: {"orders": agg_r["orders"] + 1,
                                    "total_price": agg_r["total_price"] + r["price"]},
            agg_initial_any={"orders": 0, "total_price": 0},
            project_fun=lambda key_any, agg_r: {"customer_id": key_any,
                                                "orders": agg_r["orders"],
                                                "total_price": agg_r["total_price"]},
                                                trigger_positive_only_bool=False
        ).sink(self.sink_str)
        #
        built_tn = Tn.build(sink_tn)
        built_tn.from_zSet(Tn._to_records)
        #
        print("=== Step 1: An order for customer 1 arrives (price=100, ts=10). Lands in [0, 20), [0, 40), [0, 60), [0, 80), [0, 100) ===")
        # 
        self.process(built_tn, customer_id=1, price=100, ts=10, w=1)
        print("-> OK.")

        print("=== Step 2: Another order for customer 1 arrives (price=200, ts=30). Lands in [0, 40), [0, 60), [0, 80), [0, 100) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=200, ts=30, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 1, "total_price": 100, "window_end": 20}, 1)
        ])
        print("-> OK: Window [0, 20) triggered.")

        print("\n=== Step 3: An order for customer 2 arrives (price=50, ts=75). Lands in [60, 80), [80, 100) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=2, price=50, ts=75, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 2, "total_price": 300, "window_end": 40}, 1),
            ({"customer_id": 1, "orders": 2, "total_price": 300, "window_end": 60}, 1)
        ])
        print("-> OK: Windows [0, 40) and [0, 60) triggered.")

        print("\n=== Step 4: Another order from customer 1 (price=50, ts=15) arrives late (but not too late) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=50, ts=15, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 1, "total_price": 100, "window_end": 20}, -1),
            ({"customer_id": 1, "orders": 2, "total_price": 150, "window_end": 20}, 1),
            
            ({"customer_id": 1, "orders": 2, "total_price": 300, "window_end": 40}, -1),
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 40}, 1),
            
            ({"customer_id": 1, "orders": 2, "total_price": 300, "window_end": 60}, -1),
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 60}, 1)
        ])
        print("-> OK: Corrections for windows [0, 20), [0, 40) and [0, 60) triggered.")

        print("\n=== Step 5: Another order from customer 1 (price=500, ts=105) arrives ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=500, ts=105, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 80}, 1),
            ({"customer_id": 2, "orders": 1, "total_price": 50, "window_end": 80}, 1),
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 100}, 1),
            ({"customer_id": 2, "orders": 1, "total_price": 50, "window_end": 100}, 1)
        ])
        print("-> OK: Windows [0, 80) and [0, 100] triggered.")

        print("\n=== Step 6: Yet another order from customer 1 (price=10, ts=410) arrives ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=10, ts=410, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 2, "total_price": 150, "window_end": 20}, -1),
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 40}, -1),
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 60}, -1),
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 80}, -1),
            ({"customer_id": 2, "orders": 1, "total_price": 50, "window_end": 80}, -1),
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 100}, -1),
            ({"customer_id": 2, "orders": 1, "total_price": 50, "window_end": 100}, -1),
            #    
            ({"customer_id": 1, "orders": 1, "total_price": 500, "window_end": 120}, 1),
            ({"customer_id": 1, "orders": 1, "total_price": 500, "window_end": 140}, 1),
            ({"customer_id": 1, "orders": 1, "total_price": 500, "window_end": 160}, 1),
            ({"customer_id": 1, "orders": 1, "total_price": 500, "window_end": 180}, 1),
            ({"customer_id": 1, "orders": 1, "total_price": 500, "window_end": 200}, 1)
        ])
        print("-> OK: All windows until 200) triggered.")

        print("\n=== Step 7: Another order from customer 1 (price=999, ts=10) arrives too late ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=999, ts=10, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK: Order arrived too late - no action.")

    #

    def test_sliding(self):
        size_int = 100
        #
        order_source_tn = Tn.source(self.order_source_str)
        order_source_tn.to_zSet(Tn._from_records)
        order_tn = (
            order_source_tn
            .map(lambda r: {"customer_id": r["value"]["customer_id"],
                            "price": r["value"]["price"],
                            "ts": r["value"]["ts"]})
            .expire_sliding(lambda r: r["ts"], size_int)
        )
        #
        sink_tn = order_tn.group_by_agg_sliding(
            ts_fun=lambda r: r["ts"],
            size_int=size_int,
            key_fun=lambda r: r["customer_id"],
            agg_fun=lambda agg_r, r: {"orders": agg_r["orders"] + 1,
                                    "total_price": agg_r["total_price"] + r["price"]},
            agg_initial_any={"orders": 0, "total_price": 0},
            project_fun=lambda key_any, agg_r: {"customer_id": key_any,
                                                "orders": agg_r["orders"],
                                                "total_price": agg_r["total_price"]},
            trigger_positive_only_bool=False
        ).sink(self.sink_str)
        #
        built_tn = Tn.build(sink_tn)
        built_tn.from_zSet(Tn._to_records)
        #
        print("=== Step 1: An order from customer 1 arrives (price=100, ts=10) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=100, ts=10, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 1, "total_price": 100, "window_end": 110}, 1)
        ])
        print("-> OK. Window [10, 110) triggered correctly.")

        print("\n=== Step 2: Another order from customer 1 arrives (price=200, ts=30) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=200, ts=30, w=1)
        self.assert_output(r_w_tuple_list, [
            ({'customer_id': 1, 'orders': 1, 'total_price': 100, 'window_end': 110}, -1),
            ({'customer_id': 1, 'orders': 2, 'total_price': 300, 'window_end': 110}, 1)]
        )
        print("-> OK: Correction for window [10, 110) triggered correctly.")

        print("\n=== Step 3: An order from customer 2 arrives (price=50, ts=75) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=2, price=50, ts=75, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 2, "orders": 1, "total_price": 50, "window_end": 175}, 1)
        ])
        print("-> OK: Window [75, 175) triggered correctly.")

        print("\n=== Step 4: Another order from customer 1 arrives late but still inside [10, 110) (price=50, ts=15) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=50, ts=15, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 2, "total_price": 300, "window_end": 110}, -1),
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 110}, 1)
        ])
        print("-> OK: Window [10, 110) retracted correctly; window [15, 11ß) triggered correctly.")

        print("\n=== Step 5: Yet another order from customer 1 arrives (not inside [15, 115) (price=500, ts=200) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=500, ts=200, w=1)
        self.assert_output(r_w_tuple_list, [
            ({'customer_id': 1, 'orders': 3, 'total_price': 350, 'window_end': 110}, -1),
            ({'customer_id': 2, 'orders': 1, 'total_price': 50, 'window_end': 175}, -1),
            ({'customer_id': 1, 'orders': 1, 'total_price': 500, 'window_end': 300}, 1)
        ])
        print("-> OK: Old windows [15, 115) and [75, 175) retracted correctly; wew window [200, 300) triggered correctly.")

        print("\n=== Step 6: And yet another order from customer 1 arrives too late ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=999, ts=5, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK: Order arrived too late - no action.")

    #

    def test_session(self):
        ts_step_int = 1
        gap_int = ts_step_int * 20
        max_session_int = ts_step_int * 200
        allowed_lateness_int = gap_int * 3
        #
        order_source_tn = Tn.source(self.order_source_str)
        order_source_tn.to_zSet(Tn._from_records)
        order_tn = (
            order_source_tn
            .map(lambda r: {"customer_id": r["value"]["customer_id"],
                            "price": r["value"]["price"],
                            "ts": r["value"]["ts"]})
            .expire_session(lambda r: r["ts"], max_session_int, allowed_lateness_int)
            .distinct()
        )
        #
        sink_tn = order_tn.group_by_agg_session(
            ts_fun=lambda r: r["ts"],
            gap_int=gap_int,
            key_fun=lambda r: r["customer_id"],
            agg_fun=lambda agg_r, r: {"orders": agg_r["orders"] + 1,           
                                    "total_price": agg_r["total_price"] + r["price"]},
            agg_initial_any={"orders": 0, "total_price": 0},
            project_fun=lambda key_any, agg_r: {"customer_id": key_any,
                                                "orders": agg_r["orders"],
                                                "total_price": agg_r["total_price"]},
                                                trigger_positive_only_bool=False
        ).sink(self.sink_str)
        #
        built_tn = Tn.build(sink_tn)
        built_tn.from_zSet(Tn._to_records)
        #
        print("=== Step 1: An order from customer 1 arrives (price=100, ts=10) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=100, ts=10, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK: Window [10, 30) not yet triggered.")

        print("\n=== Step 2: Another order from customer 1 arrives (price=200, ts=25) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=200, ts=25, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK: Window [25, 45) also not yet triggered but merged with [10, 30) => [10, 45).")

        print("\n=== Step 3: An order from customer 2 arrives (price=50, ts=75) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=2, price=50, ts=75, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 2, "total_price": 300, "window_end": 45}, 1)
        ])
        print("-> OK: Window [10, 45) triggered.")

        print("\n=== Step 4: Another order from customer 1 arrives (price=50, ts=15) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=50, ts=15, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 2, "total_price": 300, "window_end": 45}, -1),
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 45}, 1)
        ])
        print("-> OK: Correction for window [10, 45) triggered.")

        print("\n=== Step 5: Yet another order from customer 1 arrives (price=500, ts=200) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=500, ts=200, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 2, "orders": 1, "total_price": 50, "window_end": 95}, 1)
        ])
        print("-> OK: Window [75, 95) triggered.")

        print("\n=== Step 6: And yet another order from customer 1 arrives late but not too late (price=999, ts=1) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=999, ts=1, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 45}, -1),
            ({"customer_id": 1, "orders": 4, "total_price": 1349, "window_end": 45}, 1)
        ])
        print("-> OK: Window [10, 45) retracted; New window [1, 45) triggered.")

        print("\n=== Step 7: Yet another order from customer 1 arrives (price=100, ts=300) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=100, ts=300, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 4, "total_price": 1349, "window_end": 45}, -1),
            ({"customer_id": 2, "orders": 1, "total_price": 50, "window_end": 95}, -1),
            ({"customer_id": 1, "orders": 1, "total_price": 500, "window_end": 220}, 1)
        ])
        print("-> OK: Windows [1, 45) and [75, 95) retracted; window [200, 220) triggered.")

        print("\n=== Step 8: An order from from customer 2 arrives too late (price=200, ts=2) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=2, price=200, ts=2, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK: Order arrived too late - no action.")

    #

    def test_threshold(self):
        ts_step_int = 1
        gap_int = ts_step_int * 20
        max_session_int = ts_step_int * 200
        allowed_lateness_int = gap_int * 3
        #
        order_source_tn = Tn.source(self.order_source_str)
        order_source_tn.to_zSet(Tn._from_records)
        order_tn = (
            order_source_tn
            .map(lambda r: {"customer_id": r["value"]["customer_id"],
                            "price": r["value"]["price"],
                            "ts": r["value"]["ts"]})
            .expire_session(lambda r: r["ts"], max_session_int, allowed_lateness_int)
            .distinct()
        )
        #
        sink_tn = order_tn.group_by_agg_session(
            ts_fun=lambda r: r["ts"],
            gap_int=gap_int,
            key_fun=lambda r: r["customer_id"], 
            agg_fun=lambda agg_r, r: {"orders": agg_r["orders"] + 1,           
                                    "total_price": agg_r["total_price"] + r["price"]},
            agg_initial_any={"orders": 0, "total_price": 0},
            project_fun=lambda key_any, agg_r: {"customer_id": key_any,
                                                "orders": agg_r["orders"],
                                                "total_price": agg_r["total_price"]},
            trigger_fun=lambda r_end_ts_tuple, latest_ts: latest_ts >= r_end_ts_tuple[1] or r_end_ts_tuple[0]["total_price"] > 200, 
            trigger_positive_only_bool=False
        ).sink(self.sink_str)
        #
        built_tn = Tn.build(sink_tn)
        built_tn.from_zSet(Tn._to_records)
        #
        print("=== Step 1: An order from customer 1 arrives (price=100, ts=10) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=100, ts=10, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK: Window [10, 30) not yet triggered.")

        print("\n=== Step 2: Another order from customer 1 arrives (price=200, ts=25) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=200, ts=25, w=1)
        self.assert_output(r_w_tuple_list, [
            ({'customer_id': 1, 'orders': 2, 'total_price': 300, 'window_end': 45}, 1)
        ])
        print("-> OK: Window [25, 45) - and already triggered since total_price >= 300.")

        print("\n=== Step 3: An order from customer 2 arrives (price=50, ts=75) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=2, price=50, ts=75, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK: Window [10, 45) triggered.")

        print("\n=== Step 4: Another order from customer 1 arrives (price=50, ts=15) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=50, ts=15, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 2, "total_price": 300, "window_end": 45}, -1),
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 45}, 1)
        ])
        print("-> OK: Correction for window [10, 45) triggered.")

        print("\n=== Step 5: Yet another order from customer 1 arrives (price=500, ts=200) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=500, ts=200, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 2, "orders": 1, "total_price": 50, "window_end": 95}, 1),
            ({'customer_id': 1, 'orders': 1, 'total_price': 500, 'window_end': 220}, 1)
        ])
        print("-> OK: Windows [75, 95) and [200, 220) triggered (the latter has total price >=300).")

        print("\n=== Step 6: And yet another order from customer 1 arrives late but not too late (price=999, ts=1) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=999, ts=1, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 3, "total_price": 350, "window_end": 45}, -1),
            ({"customer_id": 1, "orders": 4, "total_price": 1349, "window_end": 45}, 1)
        ])
        print("-> OK: Window [10, 45) retracted; New window [1, 45) triggered.")

        print("\n=== Step 7: Yet another order from customer 1 arrives (price=100, ts=300) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=1, price=100, ts=300, w=1)
        self.assert_output(r_w_tuple_list, [
            ({"customer_id": 1, "orders": 4, "total_price": 1349, "window_end": 45}, -1),
            ({"customer_id": 2, "orders": 1, "total_price": 50, "window_end": 95}, -1),
        ])
        print("-> OK: Windows [1, 45) and [75, 95) retracted; window [200, 220) triggered.")

        print("\n=== Step 8: An order from from customer 2 arrives too late (price=200, ts=2) ===")
        r_w_tuple_list = self.process(built_tn, customer_id=2, price=200, ts=2, w=1)
        self.assert_output(r_w_tuple_list, [])
        print("-> OK: Order arrived too late - no action.")

    #

    def process(self, built_tn, customer_id, price, ts, w=1):
        m = {"value": {"customer_id": customer_id, "price": price, "ts": ts}}
        #
        sink_str_r_w_tuple_list_dict = built_tn.process({self.order_source_str: [(m, w)]})
        r_w_tuple_list = sink_str_r_w_tuple_list_dict[self.sink_str]
        r_w_tuple_list = [(r, w) for r, w in r_w_tuple_list if w != 0]
        #
        return r_w_tuple_list

    def assert_output(self, actual_r_w_tuple_list, expected_r_w_tuple_list):
        def sort_key(r_w_tuple):
            r, w = r_w_tuple
            return (r.get("window_end", 0), r.get("customer_id", 0), w)
        #
        actual_sorted_r_w_tuple_list = sorted(actual_r_w_tuple_list, key=sort_key)
        expected_sorted_r_w_tuple_list = sorted(expected_r_w_tuple_list, key=sort_key)
        #
        self.assertEqual(actual_sorted_r_w_tuple_list,  expected_sorted_r_w_tuple_list)

#

class OrderGenerator:
    def __init__(self, customers_int=10, ts_int=0, ts_step_int=1):
        self.customers_int = customers_int
        self.customer_id_int = 0
        #
        self.ts_int = ts_int
        self.ts_step_int = ts_step_int

    def generate(self, n=1):
        m_list = []
        for _ in range(n):
            order_id_str = str(uuid.uuid4())
            m = {
                "key": order_id_str,
                "value": {"order_id": order_id_str,
                        "customer_id": random.randint(0, self.customers_int - 1),
                        "price": random.randint(1, 10000) / 100,
                        "ts": self.ts_int},
            }
            #
            self.ts_int += self.ts_step_int
            #
            m_list.append(m)
        #
        return m
