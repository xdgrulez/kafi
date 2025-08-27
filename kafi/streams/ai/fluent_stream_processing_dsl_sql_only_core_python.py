# fluent_sql_dsl.py

# === Expression Layer ===

class Expr:
    def __and__(self, other): return BinaryOp("AND", self, ensure_expr(other))
    def __or__(self, other): return BinaryOp("OR", self, ensure_expr(other))
    def __eq__(self, other): return BinaryOp("=", self, ensure_expr(other))
    def __ne__(self, other): return BinaryOp("<>", self, ensure_expr(other))
    def __gt__(self, other): return BinaryOp(">", self, ensure_expr(other))
    def __ge__(self, other): return BinaryOp(">=", self, ensure_expr(other))
    def __lt__(self, other): return BinaryOp("<", self, ensure_expr(other))
    def __le__(self, other): return BinaryOp("<=", self, ensure_expr(other))
    def __add__(self, other): return BinaryOp("+", self, ensure_expr(other))
    def __sub__(self, other): return BinaryOp("-", self, ensure_expr(other))
    def __mul__(self, other): return BinaryOp("*", self, ensure_expr(other))
    def __truediv__(self, other): return BinaryOp("/", self, ensure_expr(other))
    def sql(self) -> str: raise NotImplementedError


class Literal(Expr):
    def __init__(self, value, type=None):
        self.value = value
        self.type = type
    def sql(self) -> str:
        if isinstance(self.value, str):
            return f"'{self.value}'"
        if self.value is None:
            return "NULL"
        return str(self.value)


class Column(Expr):
    def __init__(self, name: str):
        self.name = name
    def sql(self) -> str: return self.name


class BinaryOp(Expr):
    def __init__(self, op: str, left: Expr, right: Expr):
        self.op, self.left, self.right = op, left, right
    def sql(self) -> str: return f"({self.left.sql()} {self.op} {self.right.sql()})"


class Func(Expr):
    def __init__(self, name: str, *args: Expr):
        self.name = name.upper()
        self.args = [ensure_expr(a) for a in args]
    def sql(self) -> str:
        return f"{self.name}({', '.join(a.sql() for a in self.args)})"


def ensure_expr(val) -> Expr:
    if isinstance(val, Expr): return val
    return Literal(val)

def col(name: str) -> Column: return Column(name)
def lit(value) -> Literal: return Literal(value)
def fn(name: str, *args) -> Func: return Func(name, *args)

# aggregates
def sum_(expr): return fn("SUM", expr)
def count(expr=None): return fn("COUNT", expr if expr is not None else Literal("*"))
def avg(expr): return fn("AVG", expr)
def min_(expr): return fn("MIN", expr)
def max_(expr): return fn("MAX", expr)


# === Logical Plan Layer ===

class PlanNode: pass

class SourceNode(PlanNode):
    def __init__(self, name, schema=None): self.name, self.schema = name, schema

class ProjectNode(PlanNode):
    def __init__(self, input, projections): self.input, self.projections = input, projections

class FilterNode(PlanNode):
    def __init__(self, input, predicate): self.input, self.predicate = input, predicate

class JoinNode(PlanNode):
    def __init__(self, left, right, on, how="inner"):
        self.left, self.right, self.on, self.how = left, right, on, how

class GroupAggNode(PlanNode):
    def __init__(self, input, keys, aggs, having=None):
        self.input, self.keys, self.aggs, self.having = input, keys, aggs, having

class DistinctNode(PlanNode):
    def __init__(self, input): self.input = input

class OrderByNode(PlanNode):
    def __init__(self, input, keys): self.input, self.keys = input, keys

class LimitNode(PlanNode):
    def __init__(self, input, limit, offset=None):
        self.input, self.limit, self.offset = input, limit, offset

class UnionNode(PlanNode):
    def __init__(self, left, right, all=False):
        self.left, self.right, self.all = left, right, all

class SinkNode(PlanNode):
    def __init__(self, input, name): self.input, self.name = input, name


# === Stream DSL Wrapper ===

class Stream:
    def __init__(self, plan: PlanNode, schema=None, key=None):
        self._plan, self._schema, self._key = plan, schema, key

    def select(self, *exprs): return Stream(ProjectNode(self._plan, exprs))
    def where(self, predicate): return Stream(FilterNode(self._plan, ensure_expr(predicate)))
    def join(self, other, on, how="inner"): return Stream(JoinNode(self._plan, other._plan, ensure_expr(on), how))
    def group_by(self, *keys): return GroupedStream(self, keys)
    def distinct(self): return Stream(DistinctNode(self._plan))
    def order_by(self, *keys): return Stream(OrderByNode(self._plan, keys))
    def limit(self, n, offset=None): return Stream(LimitNode(self._plan, n, offset))
    def union(self, other, all=False): return Stream(UnionNode(self._plan, other._plan, all))
    def to(self, name): return Stream(SinkNode(self._plan, name))

class GroupedStream:
    def __init__(self, parent: Stream, keys): self.parent, self.keys = parent, keys
    def agg(self, *aggs, having=None): return Stream(GroupAggNode(self.parent._plan, self.keys, aggs, having))


# === SQL Compiler ===

def compile_to_sql(plan: PlanNode) -> str:
    if isinstance(plan, SourceNode):
        return f"(SELECT * FROM {plan.name})"
    if isinstance(plan, ProjectNode):
        child = compile_to_sql(plan.input)
        return f"SELECT {', '.join(e.sql() for e in plan.projections)} FROM {child}"
    if isinstance(plan, FilterNode):
        child = compile_to_sql(plan.input)
        return f"SELECT * FROM {child} WHERE {plan.predicate.sql()}"
    if isinstance(plan, JoinNode):
        left, right = compile_to_sql(plan.left), compile_to_sql(plan.right)
        return f"{left} {plan.how.upper()} JOIN {right} ON {plan.on.sql()}"
    if isinstance(plan, GroupAggNode):
        child = compile_to_sql(plan.input)
        keys_sql = ", ".join(k.sql() for k in plan.keys)
        aggs_sql = ", ".join(a.sql() for a in plan.aggs)
        sql = f"SELECT {keys_sql}, {aggs_sql} FROM {child} GROUP BY {keys_sql}"
        if plan.having: sql += f" HAVING {plan.having.sql()}"
        return sql
    if isinstance(plan, DistinctNode):
        child = compile_to_sql(plan.input)
        return f"SELECT DISTINCT * FROM {child}"
    if isinstance(plan, OrderByNode):
        child = compile_to_sql(plan.input)
        return f"{child} ORDER BY {', '.join(k.sql() for k in plan.keys)}"
    if isinstance(plan, LimitNode):
        child = compile_to_sql(plan.input)
        return f"{child} LIMIT {plan.limit}" + (f" OFFSET {plan.offset}" if plan.offset else "")
    if isinstance(plan, UnionNode):
        left, right = compile_to_sql(plan.left), compile_to_sql(plan.right)
        return f"{left} UNION{' ALL' if plan.all else ''} {right}"
    if isinstance(plan, SinkNode):
        child = compile_to_sql(plan.input)
        return f"INSERT INTO {plan.name} {child}"
    raise NotImplementedError(f"Unknown node type {type(plan)}")


# === Demo ===

def demo():
    orders = Stream(SourceNode("orders"))
    result = (
        orders
        .where((col("amount") > 100) & (col("country") == "DE"))
        .group_by(col("customer"))
        .agg(sum_(col("amount")), count())
        .order_by(col("customer"))
        .limit(10)
        .to("big_orders")
    )
    sql = compile_to_sql(result._plan)
    print(sql)


if __name__ == "__main__":
    demo()
