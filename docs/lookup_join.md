# Lookup joins

`lookup_join()` enriches incoming left records using the current right-side
state. The operator retains the right relation, without retaining earlier left
records. Right-side changes do not retract or reproduce earlier lookup results;
a left record without a match is discarded and is not retried later.

Use it for events that should consult a changing reference table when processed.
Use `join()` when changes on either side should update the joined relation.

## Example

This example runs entirely in memory and does not need Kafka:

```python
from kafi.streams.topologynode import TopologyNode as Tn

profiles = Tn.source("profiles").compact()
enriched = Tn.source("events").lookup_join(
    profiles,
    left_key_fun=lambda event: event["user_id"],
    right_key_fun=lambda profile: profile["key"],
    project_fun=lambda event, profile: {
        "event_id": event["event_id"],
        "tier": profile["value"]["tier"],
    },
).sink("enriched")
pipeline = Tn.build(enriched)

# Right changes in a step are applied before that step's left lookups.
assert pipeline.process({
    "profiles": [{"key": "u1", "value": {"tier": "basic"}}],
    "events": [{"event_id": "e1", "user_id": "u1"}],
})["enriched"] == [{"event_id": "e1", "tier": "basic"}]

# Updating the reference does not reproduce e1.
assert pipeline.process({
    "profiles": [{"key": "u1", "value": {"tier": "pro"}}],
})["enriched"] == []
assert pipeline.process({
    "events": [{"event_id": "e2", "user_id": "u1"}],
})["enriched"] == [{"event_id": "e2", "tier": "pro"}]

# compact() turns a tombstone into removal from the lookup table.
assert pipeline.process({
    "profiles": [{"key": "u1", "value": None}],
})["enriched"] == []
assert pipeline.process({
    "events": [{"event_id": "e3", "user_id": "u1"}],
})["enriched"] == []

# Restoring the profile does not retry the unmatched e3.
assert pipeline.process({
    "profiles": [{"key": "u1", "value": {"tier": "basic"}}],
})["enriched"] == []
```

A table can also be filtered before the lookup, for example
`Tn.source("profiles").compact().filter(lambda r: r["value"]["active"])`.
When an update stops satisfying the predicate, the filter retracts that record
from the lookup state. Earlier events still produce no new output.

## Input contract

- **Processing order:** "current" means right-side inputs processed so far,
  including changes in the current step. The operator does not align event
  timestamps or establish ordering across sources; callers choose step boundaries.
- **Relation changes:** the right input consists of weighted additions and
  retractions, rather than repeated full snapshots. For changelog records,
  `compact()` maintains the latest value per key and handles tombstones.
- **Updates within a batch:** input normalization creates a Z-set, so a batch
  cannot represent arbitrary ordered transitions such as A, B, A for one key.
  Process order-sensitive updates in separate steps. If only the batch's final
  table state matters, select the final update per key before passing the batch
  to `compact()`.
- **Matching and weights:** all live right records with the same key participate;
  left and right weights multiply, and identical projected outputs consolidate.
  For table behavior, maintain at most one live right record of weight `1` per
  key, as provided by `compact()`.
- **Repeated events:** plain-record input coalesces identical records within a
  step. Include distinct event IDs, or configure the source with
  `.to_zSet(Tn._from_records)` and pass one `(record, total_weight)` pair per
  distinct record after aggregating multiplicity. Use
  `.from_zSet(Tn._to_records)` to inspect output weights directly.
- **Append-only left input:** negative left weights raise `ValueError`, including
  when the record has no matching right key. Reset or restore the topology before
  processing another step after this error.

## Regression tests

From the repository root, with the project dependencies installed:

```sh
python -m unittest test.streams.test_lookup_join
```
