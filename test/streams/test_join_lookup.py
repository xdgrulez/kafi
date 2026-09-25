import unittest

from kafi.streams.topologynode import TopologyNode as Tn


class TestJoinLookup(unittest.TestCase):
    fact_source_str = "facts"
    product_source_str = "products"
    sink_str = "joined"

    @staticmethod
    def _fact(event_id, product_id="P"):
        return {"event_id": event_id, "product_id": product_id}

    @staticmethod
    def _product(name, product_id="P"):
        return {"key": product_id, "value": {"name": name}}

    @staticmethod
    def _tombstone(product_id="P"):
        return {"key": product_id, "value": None}

    def _build(self, *, compact_right=True, weighted_left=False, weighted_right=False, project_fun=None, right_filter=None):
        left_tn = Tn.source(self.fact_source_str)
        if weighted_left:
            left_tn.to_zSet(Tn._from_records)
        #
        right_tn = Tn.source(self.product_source_str)
        if weighted_right:
            right_tn.to_zSet(Tn._from_records)
        if compact_right:
            right_tn = right_tn.compact()
        if right_filter is not None:
            right_tn = right_tn.filter(right_filter)
        #
        if project_fun is None:
            project_fun = lambda fact, product: {
                "event_id": fact["event_id"],
                "product_name": product["value"]["name"],
            }
        #
        sink_tn = (
            left_tn
            .join_lookup(
                right_tn,
                left_key_fun=lambda fact: fact["product_id"],
                right_key_fun=lambda product: product["key"],
                project_fun=project_fun,
            )
            .sink(self.sink_str)
        )
        built_tn = Tn.build(sink_tn)
        built_tn.from_zSet(Tn._to_records)
        return built_tn

    def _process(self, built_tn, *, facts=None, products=None):
        input_dict = {}
        if facts is not None:
            input_dict[self.fact_source_str] = facts
        if products is not None:
            input_dict[self.product_source_str] = products
        #
        return built_tn.process(input_dict)[self.sink_str]

    def test_missing_product_is_not_replayed_when_product_appears_later(self):
        built_tn = self._build()

        self.assertEqual(self._process(built_tn, facts=[self._fact("A")]), [])
        self.assertEqual(self._process(built_tn, products=[self._product("v1")]), [])
        self.assertEqual(
            self._process(built_tn, facts=[self._fact("B")]),
            [({"event_id": "B", "product_name": "v1"}, 1)],
        )
        self.assertEqual(self._process(built_tn, products=[self._tombstone()]), [])
        self.assertEqual(self._process(built_tn, facts=[self._fact("C")]), [])
        self.assertEqual(self._process(built_tn, products=[self._product("v2")]), [])
        self.assertEqual(
            self._process(built_tn, facts=[self._fact("D")]),
            [({"event_id": "D", "product_name": "v2"}, 1)],
        )

    def test_right_update_does_not_retract_or_replay_historical_left_records(self):
        built_tn = self._build()

        self.assertEqual(self._process(built_tn, products=[self._product("v1")]), [])
        self.assertEqual(
            self._process(built_tn, facts=[self._fact("A")]),
            [({"event_id": "A", "product_name": "v1"}, 1)],
        )
        self.assertEqual(self._process(built_tn, products=[self._product("v2")]), [])
        self.assertEqual(
            self._process(built_tn, facts=[self._fact("B")]),
            [({"event_id": "B", "product_name": "v2"}, 1)],
        )

    def test_same_step_uses_the_updated_right_state(self):
        built_tn = self._build()

        self.assertEqual(
            self._process(
                built_tn,
                facts=[self._fact("A")],
                products=[self._product("v1")],
            ),
            [({"event_id": "A", "product_name": "v1"}, 1)],
        )
        self.assertEqual(
            self._process(
                built_tn,
                facts=[self._fact("B")],
                products=[self._product("v2")],
            ),
            [({"event_id": "B", "product_name": "v2"}, 1)],
        )
        self.assertEqual(
            self._process(
                built_tn,
                facts=[self._fact("C")],
                products=[self._tombstone()],
            ),
            [],
        )

    def test_lookup_uses_only_right_records_with_the_same_key(self):
        built_tn = self._build()

        self.assertEqual(
            self._process(
                built_tn,
                products=[self._product("p", "P"), self._product("q", "Q")],
            ),
            [],
        )
        self.assertEqual(
            self._process(built_tn, facts=[self._fact("A", "Q")]),
            [({"event_id": "A", "product_name": "q"}, 1)],
        )
        self.assertEqual(
            self._process(built_tn, facts=[self._fact("B", "missing")]),
            [],
        )

    def test_left_weights_are_preserved_and_negative_weights_are_rejected(self):
        built_tn = self._build(weighted_left=True)
        self._process(built_tn, products=[self._product("v1")])

        self.assertEqual(
            self._process(built_tn, facts=[(self._fact("A"), 2)]),
            [({"event_id": "A", "product_name": "v1"}, 2)],
        )
        with self.assertRaisesRegex(ValueError, "append-only left input"):
            self._process(built_tn, facts=[(self._fact("A"), -1)])

        unmatched_tn = self._build(weighted_left=True)
        with self.assertRaisesRegex(ValueError, "append-only left input"):
            self._process(
                unmatched_tn,
                facts=[(self._fact("B", "missing"), -1)],
            )

    def test_right_differential_state_and_projection_consolidation(self):
        built_tn = self._build(
            compact_right=False,
            weighted_left=True,
            weighted_right=True,
            project_fun=lambda _fact, _product: {"matched": True},
        )
        product_v1 = self._product("v1")
        product_v2 = self._product("v2")

        self.assertEqual(self._process(built_tn, products=[(product_v1, 2)]), [])
        self.assertEqual(
            self._process(
                built_tn,
                facts=[(self._fact("A"), 1), (self._fact("B"), 1)],
            ),
            [({"matched": True}, 4)],
        )
        self.assertEqual(self._process(built_tn, products=[(product_v1, -2)]), [])
        self.assertEqual(self._process(built_tn, facts=[(self._fact("C"), 1)]), [])
        self.assertEqual(
            self._process(
                built_tn,
                facts=[(self._fact("D"), 1)],
                products=[(product_v2, 1)],
            ),
            [({"matched": True}, 1)],
        )

    def test_reset_and_state_restore_on_same_topology_preserve_right_snapshot(self):
        built_tn = self._build()
        self._process(built_tn, products=[self._product("v1")])
        saved_state = built_tn.save_state()

        built_tn.reset()
        self.assertEqual(self._process(built_tn, facts=[self._fact("A")]), [])

        built_tn.load_state(saved_state)
        self.assertEqual(
            self._process(built_tn, facts=[self._fact("B")]),
            [({"event_id": "B", "product_name": "v1"}, 1)],
        )

    def test_multiple_right_records_per_key_are_independently_retracted(self):
        built_tn = self._build(compact_right=False, weighted_right=True)
        product_v1 = self._product("v1")
        product_v2 = self._product("v2")

        self.assertEqual(
            self._process(built_tn, products=[(product_v1, 1), (product_v2, 2)]),
            [],
        )
        self.assertCountEqual(
            self._process(built_tn, facts=[self._fact("A")]),
            [
                ({"event_id": "A", "product_name": "v1"}, 1),
                ({"event_id": "A", "product_name": "v2"}, 2),
            ],
        )
        self.assertEqual(self._process(built_tn, products=[(product_v1, -1)]), [])
        self.assertEqual(
            self._process(built_tn, facts=[self._fact("B")]),
            [({"event_id": "B", "product_name": "v2"}, 2)],
        )

    def test_filtered_compacted_update_removes_lookup_membership_without_replay(self):
        built_tn = self._build(right_filter=lambda product: product["value"]["name"] == "included")

        self.assertEqual(
            self._process(built_tn, products=[self._product("included")], facts=[self._fact("A")]),
            [({"event_id": "A", "product_name": "included"}, 1)],
        )
        self.assertEqual(
            self._process(built_tn, products=[self._product("excluded")], facts=[self._fact("B")]),
            [],
        )
        self.assertEqual(self._process(built_tn, products=[self._product("included")]), [])
        self.assertEqual(
            self._process(built_tn, facts=[self._fact("C")]),
            [({"event_id": "C", "product_name": "included"}, 1)],
        )


if __name__ == "__main__":
    unittest.main()
