# fluent_stream_sql_dsl.py
# Komplettes, eigenständiges Mini-Framework für SQL-ähnliche Stream-Topologien
# mit Ausgabe als SQL-String und als Mermaid-Flowchart.

# =======================
# Expression Layer
# =======================

class Expr:
    # boolean
    def __and__(self, other): return BinOp("AND", self, ensure_expr(other))
    def __or__(self, other):  return BinOp("OR",  self, ensure_expr(other))
    # compare
    def __eq__(self, other):  return BinOp("=",   self, ensure_expr(other))
    def __ne__(self, other):  return BinOp("<>",  self, ensure_expr(other))
    def __gt__(self, other):  return BinOp(">",   self, ensure_expr(other))
    def __ge__(self, other):  return BinOp(">=",  self, ensure_expr(other))
    def __lt__(self, other):  return BinOp("<",   self, ensure_expr(other))
    def __le__(self, other):  return BinOp("<=",  self, ensure_expr(other))
    # arithmetic
    def __add__(self, other): return BinOp("+",   self, ensure_expr(other))
    def __sub__(self, other): return BinOp("-",   self, ensure_expr(other))
    def __mul__(self, other): return BinOp("*",   self, ensure_expr(other))
    def __truediv__(self, other): return BinOp("/", self, ensure_expr(other))

    # ordering sugar
    def asc(self):  return OrderKey(self, True)
    def desc(self): return OrderKey(self, False)

    def alias(self, name: str): return Alias(self, name)

    def sql(self) -> str: raise NotImplementedError


class Literal(Expr):
    def __init__(self, value, typ=None):
        self.value = value
        self.typ = typ
    def sql(self) -> str:
        if self.value is None:
            return "NULL"
        if isinstance(self.value, str):
            return f"'{self.value}'"
        return str(self.value)


class Star(Expr):
    """Rendert ein unquotiertes * (z.B. für COUNT(*) oder SELECT *)."""
    def sql(self) -> str: return "*"


class Column(Expr):
    def __init__(self, name: str):
        self.name = name
    def sql(self) -> str:
        # bewusst ohne Quotes gehalten; bei Bedarf anpassen
        return self.name


class BinOp(Expr):
    def __init__(self, op: str, left: Expr, right: Expr):
        self.op, self.left, self.right = op, left, right
    def sql(self) -> str:
        return f"({self.left.sql()} {self.op} {self.right.sql()})"


class Func(Expr):
    def __init__(self, name: str, *args: Expr):
        self.name = name.upper()
        self.args = [ensure_expr(a) for a in args]
    def sql(self) -> str:
        return f"{self.name}({', '.join(a.sql() for a in self.args)})"


class Alias(Expr):
    def __init__(self, expr: Expr, name: str):
        self.expr, self.name = expr, name
    def sql(self) -> str:
        return f"{self.expr.sql()} AS {self.name}"


class OrderKey:
    def __init__(self, expr: Expr, ascending: bool = True):
        self.expr, self.ascending = expr, ascending
    def sql(self) -> str:
        return f"{self.expr.sql()} {'ASC' if self.ascending else 'DESC'}"


def ensure_expr(val) -> Expr:
    if isinstance(val, Expr): return val
    return Literal(val)

def col(name: str) -> Column: return Column(name)
def lit(value) -> Literal:    return Literal(value)
def star() -> Star:           return Star()
def fn(name: str, *args) -> Func: return Func(name, *args)

# Aggregat-Helfer
def sum_(expr) -> Expr:  return fn("SUM", expr)
def count(expr=None) -> Expr:  return fn("COUNT", expr if expr is not None else star())
def avg(expr) -> Expr:   return fn("AVG", expr)
def min_(expr) -> Expr:  return fn("MIN", expr)
def max_(expr) -> Expr:  return fn("MAX", expr)


# =======================
# Logical Plan Layer
# =======================

class PlanNode: pass

class SourceNode(PlanNode):
    def __init__(self, name, schema=None, key=None):
        self.name, self.schema, self.key = name, (schema or {}), key

class ProjectNode(PlanNode):
    def __init__(self, input, projections):
        self.input, self.projections = input, list(projections)

class FilterNode(PlanNode):
    def __init__(self, input, predicate: Expr):
        self.input, self.predicate = input, ensure_expr(predicate)

class JoinNode(PlanNode):
    def __init__(self, left, right, on: Expr, how="inner"):
        self.left, self.right, self.on, self.how = left, right, ensure_expr(on), how

class GroupAggNode(PlanNode):
    def __init__(self, input, keys, aggs, having=None):
        self.input = input
        self.keys = [ensure_expr(k) for k in keys]
        self.aggs = [ensure_expr(a) for a in aggs]
        self.having = ensure_expr(having) if having is not None else None

class DistinctNode(PlanNode):
    def __init__(self, input): self.input = input

class OrderByNode(PlanNode):
    def __init__(self, input, keys):
        self.input = input
        # keys können Expr (-> ASC) oder OrderKey sein
        ok = []
        for k in keys:
            ok.append(k if isinstance(k, OrderKey) else OrderKey(ensure_expr(k), True))
        self.keys = ok

class LimitNode(PlanNode):
    def __init__(self, input, limit, offset=None):
        self.input, self.limit, self.offset = input, int(limit), (int(offset) if offset else None)

class UnionNode(PlanNode):
    def __init__(self, left, right, all=False):
        self.left, self.right, self.all = left, right, bool(all)

class SinkNode(PlanNode):
    def __init__(self, input, name): self.input, self.name = input, name


# =======================
# Stream DSL Wrapper
# =======================

class Stream:
    def __init__(self, plan: PlanNode, schema=None, key=None):
        self._plan, self._schema, self._key = plan, (schema or {}), key

    @staticmethod
    def source(name: str, schema=None, key=None):
        return Stream(SourceNode(name, schema=schema, key=key), schema, key)

    # relational ops
    def select(self, *exprs):
        exprs = [ensure_expr(e) for e in (exprs if exprs else [star()])]
        return Stream(ProjectNode(self._plan, exprs), self._schema, self._key)

    def where(self, predicate):
        return Stream(FilterNode(self._plan, ensure_expr(predicate)), self._schema, self._key)

    def join(self, other, on, how="inner"):
        return Stream(JoinNode(self._plan, other._plan, ensure_expr(on), how), self._schema, self._key)

    def left_join(self, other, on):  return self.join(other, on, "left")
    def right_join(self, other, on): return self.join(other, on, "right")
    def full_join(self, other, on):  return self.join(other, on, "full")
    def semi_join(self, other, on):  return self.join(other, on, "semi")
    def anti_join(self, other, on):  return self.join(other, on, "anti")

    def group_by(self, *keys): return GroupedStream(self, keys)

    def distinct(self): return Stream(DistinctNode(self._plan), self._schema, self._key)

    def order_by(self, *keys): return Stream(OrderByNode(self._plan, keys), self._schema, self._key)

    def limit(self, n, offset=None): return Stream(LimitNode(self._plan, n, offset), self._schema, self._key)

    def union(self, other):     return Stream(UnionNode(self._plan, other._plan, all=False), self._schema, self._key)
    def union_all(self, other): return Stream(UnionNode(self._plan, other._plan, all=True),  self._schema, self._key)

    def to(self, name):  return Stream(SinkNode(self._plan, name), self._schema, self._key)  # Sink
    def sink(self, name): return self.to(name)

class GroupedStream:
    def __init__(self, parent: Stream, keys):
        self.parent, self.keys = parent, [ensure_expr(k) for k in keys]
    def agg(self, *aggs, having=None):
        return Stream(GroupAggNode(self.parent._plan, self.keys, aggs, having), self.parent._schema, self.parent._key)
    # optional sugar: group_by(...).having(...).agg(...)
    def having(self, predicate):
        return _PreHavingGrouped(self.parent, self.keys, ensure_expr(predicate))

class _PreHavingGrouped:
    def __init__(self, parent: Stream, keys, having: Expr):
        self.parent, self.keys, self.having = parent, keys, having
    def agg(self, *aggs):
        return Stream(GroupAggNode(self.parent._plan, self.keys, aggs, self.having), self.parent._schema, self.parent._key)


# =======================
# SQL Compiler
# =======================

def _wrap_as_subquery(sql: str) -> str:
    # Wenn es mit "SELECT" oder "(" beginnt, ist es bereits eine Selekt-Abfrage
    s = sql.strip().upper()
    if s.startswith("SELECT") or s.startswith("("):
        return f"({sql})"
    return f"({sql})"

def compile_to_sql(plan: PlanNode) -> str:
    if isinstance(plan, SourceNode):
        return f"SELECT * FROM {plan.name}"

    if isinstance(plan, ProjectNode):
        child = _wrap_as_subquery(compile_to_sql(plan.input))
        cols = ", ".join(e.sql() for e in plan.projections)
        return f"SELECT {cols} FROM {child}"

    if isinstance(plan, FilterNode):
        child = _wrap_as_subquery(compile_to_sql(plan.input))
        return f"SELECT * FROM {child} WHERE {plan.predicate.sql()}"

    if isinstance(plan, JoinNode):
        left  = _wrap_as_subquery(compile_to_sql(plan.left))
        right = _wrap_as_subquery(compile_to_sql(plan.right))
        how = plan.how.upper()
        return f"SELECT * FROM {left} {how} JOIN {right} ON {plan.on.sql()}"

    if isinstance(plan, GroupAggNode):
        child = _wrap_as_subquery(compile_to_sql(plan.input))
        keys_sql = ", ".join(k.sql() for k in plan.keys) if plan.keys else ""
        aggs_sql = ", ".join(a.sql() for a in plan.aggs) if plan.aggs else ""
        select_list = ", ".join([x for x in [keys_sql, aggs_sql] if x])
        sql = f"SELECT {select_list} FROM {child}"
        if plan.keys:
            sql += f" GROUP BY {keys_sql}"
        if plan.having:
            sql += f" HAVING {plan.having.sql()}"
        return sql

    if isinstance(plan, DistinctNode):
        child = _wrap_as_subquery(compile_to_sql(plan.input))
        return f"SELECT DISTINCT * FROM {child}"

    if isinstance(plan, OrderByNode):
        child = compile_to_sql(plan.input)
        order_sql = ", ".join(k.sql() for k in plan.keys)
        return f"{child} ORDER BY {order_sql}"

    if isinstance(plan, LimitNode):
        child = compile_to_sql(plan.input)
        off = f" OFFSET {plan.offset}" if plan.offset not in (None, 0) else ""
        return f"{child} LIMIT {plan.limit}{off}"

    if isinstance(plan, UnionNode):
        left  = _wrap_as_subquery(compile_to_sql(plan.left))
        right = _wrap_as_subquery(compile_to_sql(plan.right))
        return f"{left} UNION{' ALL' if plan.all else ''} {right}"

    if isinstance(plan, SinkNode):
        child = compile_to_sql(plan.input)
        return f"INSERT INTO {plan.name} {child}"

    raise NotImplementedError(f"Unknown node type {type(plan)}")


# =======================
# Mermaid Compiler
# =======================

def compile_to_mermaid(plan: PlanNode) -> str:
    """Erzeuge Mermaid-Flowchart (flowchart TD) für die Topologie."""
    lines = ["flowchart TD"]
    # node-id-zuweisung über object identity
    id_counter = [0]
    ids = {}

    def nid(node):
        if node in ids: return ids[node]
        id_counter[0] += 1
        name = f"N{id_counter[0]}"
        ids[node] = name
        return name

    def label(text: str) -> str:
        # Mini-Schutz gegen sehr lange Labels
        text = text.replace('"', "'")
        return text if len(text) < 120 else text[:117] + "..."

    def walk(node):
        me = nid(node)

        if isinstance(node, SourceNode):
            lines.append(f'{me}["Source: {node.name}"]')

        elif isinstance(node, ProjectNode):
            proj = ", ".join(e.sql() for e in node.projections)
            lines.append(f'{me}["Project: {label(proj)}"]')
            walk(node.input); lines.append(f"{nid(node.input)} --> {me}")

        elif isinstance(node, FilterNode):
            pred = node.predicate.sql()
            lines.append(f'{me}["Filter: {label(pred)}"]')
            walk(node.input); lines.append(f"{nid(node.input)} --> {me}")

        elif isinstance(node, JoinNode):
            how = node.how.upper()
            cond = node.on.sql()
            lines.append(f'{me}["Join {how}: {label(cond)}"]')
            walk(node.left);  lines.append(f"{nid(node.left)}  --> {me}")
            walk(node.right); lines.append(f"{nid(node.right)} --> {me}")

        elif isinstance(node, GroupAggNode):
            keys = ", ".join(k.sql() for k in node.keys) if node.keys else "—"
            aggs = ", ".join(a.sql() for a in node.aggs) if node.aggs else "—"
            hv   = node.having.sql() if node.having else None
            lbl  = f"GroupBy: {keys} | Agg: {aggs}" + (f" | Having: {hv}" if hv else "")
            lines.append(f'{me}["{label(lbl)}"]')
            walk(node.input); lines.append(f"{nid(node.input)} --> {me}")

        elif isinstance(node, DistinctNode):
            lines.append(f'{me}["Distinct"]')
            walk(node.input); lines.append(f"{nid(node.input)} --> {me}")

        elif isinstance(node, OrderByNode):
            oks = ", ".join(k.sql() for k in node.keys)
            lines.append(f'{me}["OrderBy: {label(oks)}"]')
            walk(node.input); lines.append(f"{nid(node.input)} --> {me}")

        elif isinstance(node, LimitNode):
            ofs = f", offset={node.offset}" if node.offset not in (None, 0) else ""
            lines.append(f'{me}["Limit: {node.limit}{ofs}"]')
            walk(node.input); lines.append(f"{nid(node.input)} --> {me}")

        elif isinstance(node, UnionNode):
            lines.append(f'{me}["Union{" ALL" if node.all else ""}"]')
            walk(node.left);  lines.append(f"{nid(node.left)}  --> {me}")
            walk(node.right); lines.append(f"{nid(node.right)} --> {me}")

        elif isinstance(node, SinkNode):
            lines.append(f'{me}["Sink: {node.name}"]')
            walk(node.input); lines.append(f"{nid(node.input)} --> {me}")

        else:
            lines.append(f'{me}["Unknown: {type(node).__name__}"]')

    walk(plan)
    return "\n".join(lines)


# =======================
# Demo
# =======================

def demo():
    orders = Stream.source("orders", schema={"order_id":"INT","cust_id":"INT","amount":"DOUBLE","country":"STRING"})
    customers = Stream.source("customers", schema={"cust_id":"INT","name":"STRING","segment":"STRING"})

    result = (
        orders
        .where((col("amount") > 100) & (col("country") == "DE"))
        .join(customers, on=(col("orders.cust_id") == col("customers.cust_id")), how="inner")
        .select(
            col("orders.order_id").alias("oid"),
            col("customers.name").alias("customer"),
            (col("amount") * 1.19).alias("amount_gross"),
        )
        .group_by(col("customer"))
        .agg(
            sum_(col("amount")).alias("net_sum"),
            count().alias("cnt"),
        )
        .order_by(col("net_sum").desc())
        .limit(10)
        .to("top_customers")
    )

    sql = compile_to_sql(result._plan)
    print("=== SQL ===")
    print(sql)

    print("\n=== Mermaid ===")
    print(compile_to_mermaid(result._plan))


# kleine Alias-Funktion für Expr.alias auf Aggregaten/Funktionen
def _expr_alias(self, name: str):
    return Alias(self, name)
# Monkey-Patching, damit man sum_(col(...)).alias("x") etc. schreiben kann
Expr.alias = _expr_alias

if __name__ == "__main__":
    demo()
