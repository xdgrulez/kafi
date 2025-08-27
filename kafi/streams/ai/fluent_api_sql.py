from __future__ import annotations
from dataclasses import dataclass
from typing import Any, List, Dict

# -----------------------
# Expressions
# -----------------------

class Expr:
    def __eq__(self, other): return BinOp("=", self, ensure_expr(other))
    def __gt__(self, other): return BinOp(">", self, ensure_expr(other))
    def __and__(self, other): return BinOp("AND", self, ensure_expr(other))

@dataclass
class Column(Expr):
    name: str
    def __repr__(self): return f"col({self.name})"

@dataclass
class Literal(Expr):
    value: Any
    def __repr__(self): return repr(self.value)

@dataclass
class BinOp(Expr):
    op: str
    left: Expr
    right: Expr
    def __repr__(self): return f"({self.left} {self.op} {self.right})"

def col(name: str) -> Column:
    return Column(name)

def lit(value: Any) -> Literal:
    return Literal(value)

def ensure_expr(val: Any) -> Expr:
    if isinstance(val, Expr):
        return val
    return lit(val)

# -----------------------
# Plan Nodes
# -----------------------

class PlanNode: pass

@dataclass
class SourceNode(PlanNode):
    name: str
    schema: Dict[str, str]

@dataclass
class FilterNode(PlanNode):
    input: PlanNode
    predicate: Expr

@dataclass
class ProjectNode(PlanNode):
    input: PlanNode
    projections: List[Expr]

@dataclass
class JoinNode(PlanNode):
    left: PlanNode
    right: PlanNode
    on: Expr
    how: str = "inner"

@dataclass
class GroupAggNode(PlanNode):
    input: PlanNode
    keys: List[Expr]
    aggs: List[Expr]

@dataclass
class SinkNode(PlanNode):
    input: PlanNode
    name: str

# -----------------------
# Stream Wrapper
# -----------------------

class Stream:
    def __init__(self, plan: PlanNode):
        self._plan = plan

    @staticmethod
    def source(name: str, schema: Dict[str, str]) -> Stream:
        return Stream(SourceNode(name, schema))

    def where(self, predicate: Expr) -> Stream:
        return Stream(FilterNode(self._plan, predicate))

    def select(self, *exprs: Expr) -> Stream:
        return Stream(ProjectNode(self._plan, list(exprs)))

    def join(self, other: Stream, on: Expr, how: str = "inner") -> Stream:
        return Stream(JoinNode(self._plan, other._plan, on, how))

    def group_by(self, *keys: Expr) -> GroupedStream:
        return GroupedStream(self._plan, list(keys))

    def to(self, name: str) -> Stream:
        return Stream(SinkNode(self._plan, name))

class GroupedStream:
    def __init__(self, plan: PlanNode, keys: List[Expr]):
        self._plan = plan
        self._keys = keys

    def agg(self, *aggs: Expr) -> Stream:
        return Stream(GroupAggNode(self._plan, self._keys, list(aggs)))

# -----------------------
# Demo
# -----------------------

def demo():
    orders = Stream.source("orders", {"id":"INT","cust_id":"INT","amount":"DOUBLE"})
    customers = Stream.source("customers", {"id":"INT","name":"STRING"})

    q = (
        orders.where(col("amount") > lit(100))
              .join(customers, on=col("cust_id") == col("id"))
              .select(col("name"), col("amount"))
              .group_by(col("name"))
              .agg(lit("SUM(amount)"))   # hier als Literal-Expr für Demo
              .to("top_customers")
    )

    print("Final plan:", q._plan)

if __name__ == "__main__":
    demo()
