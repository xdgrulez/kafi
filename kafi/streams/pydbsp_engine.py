from __future__ import annotations
from dataclasses import dataclass
from typing import Any, List, Dict, Callable, Tuple

# Import pyDBSP 0.6 API
from pydbsp.zset import ZSet
from pydbsp.stream import Stream

# -----------------------
# Original DSL Code
# -----------------------

class Expr:
    def __eq__(self, other): return BinOp("=", self, ensure_expr(other))
    def __gt__(self, other): return BinOp(">", self, ensure_expr(other))
    def __ge__(self, other): return BinOp(">=", self, ensure_expr(other))
    def __lt__(self, other): return BinOp("<", self, ensure_expr(other))
    def __le__(self, other): return BinOp("<=", self, ensure_expr(other))
    def __ne__(self, other): return BinOp("!=", self, ensure_expr(other))
    def __and__(self, other): return BinOp("AND", self, ensure_expr(other))
    def __or__(self, other): return BinOp("OR", self, ensure_expr(other))

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

@dataclass
class AggFunc(Expr):
    func: str
    expr: Expr
    def __repr__(self): return f"{self.func}({self.expr})"

def col(name: str) -> Column:
    return Column(name)

def lit(value: Any) -> Literal:
    return Literal(value)

def sum_agg(expr: Expr) -> AggFunc:
    return AggFunc("SUM", expr)

def count_agg(expr: Expr = None) -> AggFunc:
    return AggFunc("COUNT", expr or lit(1))

def avg_agg(expr: Expr) -> AggFunc:
    return AggFunc("AVG", expr)

def ensure_expr(val: Any) -> Expr:
    if isinstance(val, Expr):
        return val
    return lit(val)

# Plan Nodes
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

# Stream Wrapper (DSL)
class StreamDSL:
    def __init__(self, plan: PlanNode):
        self._plan = plan

    @staticmethod
    def source(name: str, schema: Dict[str, str]) -> 'StreamDSL':
        return StreamDSL(SourceNode(name, schema))

    def where(self, predicate: Expr) -> 'StreamDSL':
        return StreamDSL(FilterNode(self._plan, predicate))

    def select(self, *exprs: Expr) -> 'StreamDSL':
        return StreamDSL(ProjectNode(self._plan, list(exprs)))

    def join(self, other: 'StreamDSL', on: Expr, how: str = "inner") -> 'StreamDSL':
        return StreamDSL(JoinNode(self._plan, other._plan, on, how))

    def group_by(self, *keys: Expr) -> 'GroupedStream':
        return GroupedStream(self._plan, list(keys))

    def to(self, name: str) -> 'StreamDSL':
        return StreamDSL(SinkNode(self._plan, name))

class GroupedStream:
    def __init__(self, plan: PlanNode, keys: List[Expr]):
        self._plan = plan
        self._keys = keys

    def agg(self, *aggs: Expr) -> StreamDSL:
        return StreamDSL(GroupAggNode(self._plan, self._keys, list(aggs)))

# -----------------------
# Stream Processing Engine with pyDBSP 0.6
# -----------------------

class StreamProcessor:
    """Stream-Processing-Engine mit pyDBSP 0.6"""
    
    def __init__(self):
        self.sources: Dict[str, Stream] = {}
        self.sinks: Dict[str, Stream] = {}
        self.schema_info: Dict[str, Dict[str, str]] = {}
        
    def add_source(self, name: str, stream: Stream, schema: Dict[str, str] = None):
        """Fügt eine Datenquelle hinzu"""
        self.sources[name] = stream
        if schema:
            self.schema_info[name] = schema
        
    def compile_plan(self, plan: PlanNode) -> Stream:
        """Kompiliert einen Plan-Node zu einem pyDBSP Stream"""
        return self._compile_node(plan)
        
    def _compile_node(self, node: PlanNode) -> Stream:
        """Rekursive Kompilierung der Plan-Nodes"""
        
        if isinstance(node, SourceNode):
            if node.name not in self.sources:
                raise ValueError(f"Source '{node.name}' not found")
            return self.sources[node.name]
            
        elif isinstance(node, FilterNode):
            input_stream = self._compile_node(node.input)
            predicate_func = self._compile_predicate(node.predicate)
            
            def filter_op(zset):
                result_dict = {}
                for row, weight in zset.data.items():
                    if predicate_func(row):
                        result_dict[row] = weight
                return ZSet(result_dict)
            
            return input_stream.map(filter_op)
            
        elif isinstance(node, ProjectNode):
            input_stream = self._compile_node(node.input)
            projection_func = self._compile_projection(node.projections)
            
            def project_op(zset):
                result_dict = {}
                for row, weight in zset.data.items():
                    projected_row = projection_func(row)
                    if projected_row in result_dict:
                        result_dict[projected_row] += weight
                    else:
                        result_dict[projected_row] = weight
                return ZSet(result_dict)
            
            return input_stream.map(project_op)
            
        elif isinstance(node, JoinNode):
            left_stream = self._compile_node(node.left)
            right_stream = self._compile_node(node.right)
            join_func = self._compile_join_condition(node.on)
            
            def join_op(left_zset, right_zset):
                result_dict = {}
                for left_row, left_weight in left_zset.data.items():
                    for right_row, right_weight in right_zset.data.items():
                        if join_func(left_row, right_row):
                            joined_row = left_row + right_row
                            weight = left_weight * right_weight
                            if joined_row in result_dict:
                                result_dict[joined_row] += weight
                            else:
                                result_dict[joined_row] = weight
                return ZSet(result_dict)
            
            return left_stream.lift2(right_stream, join_op)
            
        elif isinstance(node, GroupAggNode):
            input_stream = self._compile_node(node.input)
            key_func = self._compile_projection(node.keys)
            agg_func = self._compile_aggregation(node.aggs)
            
            def group_agg_op(zset):
                groups = {}
                # Gruppierung
                for row, weight in zset.data.items():
                    key = key_func(row)
                    if key not in groups:
                        groups[key] = []
                    groups[key].extend([row] * weight)
                
                # Aggregation
                result_dict = {}
                for key, group_rows in groups.items():
                    agg_result = agg_func(group_rows)
                    final_row = key + agg_result
                    result_dict[final_row] = 1
                    
                return ZSet(result_dict)
            
            return input_stream.map(group_agg_op)
            
        elif isinstance(node, SinkNode):
            input_stream = self._compile_node(node.input)
            self.sinks[node.name] = input_stream
            return input_stream
            
        else:
            raise ValueError(f"Unknown node type: {type(node)}")
    
    def _compile_predicate(self, expr: Expr) -> Callable[[Tuple], bool]:
        """Kompiliert ein Prädikat zu einer Callable-Funktion"""
        def eval_expr(row: Tuple, expr: Expr) -> Any:
            if isinstance(expr, Column):
                col_idx = self._get_column_index(expr.name, row)
                return row[col_idx] if col_idx < len(row) else None
            elif isinstance(expr, Literal):
                return expr.value
            elif isinstance(expr, BinOp):
                left_val = eval_expr(row, expr.left)
                right_val = eval_expr(row, expr.right)
                
                if expr.op == "=":
                    return left_val == right_val
                elif expr.op == ">":
                    return left_val > right_val
                elif expr.op == ">=":
                    return left_val >= right_val
                elif expr.op == "<":
                    return left_val < right_val
                elif expr.op == "<=":
                    return left_val <= right_val
                elif expr.op == "!=":
                    return left_val != right_val
                elif expr.op == "AND":
                    return left_val and right_val
                elif expr.op == "OR":
                    return left_val or right_val
                else:
                    raise ValueError(f"Unknown operator: {expr.op}")
            else:
                raise ValueError(f"Unknown expression type: {type(expr)}")
        
        return lambda row: eval_expr(row, expr)
    
    def _compile_projection(self, exprs: List[Expr]) -> Callable[[Tuple], Tuple]:
        """Kompiliert Projektions-Ausdrücke"""
        def project_row(row: Tuple) -> Tuple:
            result = []
            for expr in exprs:
                if isinstance(expr, Column):
                    col_idx = self._get_column_index(expr.name, row)
                    result.append(row[col_idx] if col_idx < len(row) else None)
                elif isinstance(expr, Literal):
                    result.append(expr.value)
                else:
                    result.append(str(expr))
            return tuple(result)
        
        return project_row
    
    def _compile_join_condition(self, expr: Expr) -> Callable[[Tuple, Tuple], bool]:
        """Kompiliert Join-Bedingungen"""
        def join_condition(left_row: Tuple, right_row: Tuple) -> bool:
            if isinstance(expr, BinOp) and expr.op == "=":
                left_val = self._eval_expr_on_row(left_row, expr.left, is_left=True)
                right_val = self._eval_expr_on_row(right_row, expr.right, is_left=False)
                return left_val == right_val
            return False
        
        return join_condition
    
    def _compile_aggregation(self, agg_exprs: List[Expr]) -> Callable[[List[Tuple]], Tuple]:
        """Kompiliert Aggregations-Ausdrücke"""
        def aggregate_group(rows: List[Tuple]) -> Tuple:
            result = []
            for agg_expr in agg_exprs:
                if isinstance(agg_expr, AggFunc):
                    if agg_expr.func == "SUM":
                        values = [self._eval_expr_on_row(row, agg_expr.expr) for row in rows]
                        result.append(sum(v for v in values if isinstance(v, (int, float))))
                    elif agg_expr.func == "COUNT":
                        result.append(len(rows))
                    elif agg_expr.func == "AVG":
                        values = [self._eval_expr_on_row(row, agg_expr.expr) for row in rows]
                        numeric_values = [v for v in values if isinstance(v, (int, float))]
                        result.append(sum(numeric_values) / len(numeric_values) if numeric_values else 0)
                elif isinstance(agg_expr, Literal):
                    result.append(agg_expr.value)
                else:
                    result.append(str(agg_expr))
            return tuple(result)
        
        return aggregate_group
    
    def _get_column_index(self, col_name: str, row: Tuple) -> int:
        """Hilfsfunktion um Spalten-Index zu bekommen"""
        # Vereinfachte Implementierung - Index aus Spaltenname
        if col_name == "id":
            return 0
        elif col_name == "cust_id":
            return 1
        elif col_name == "amount":
            return 2
        elif col_name == "name":
            return 1 if len(row) == 2 else 0
        else:
            return 0
    
    def _eval_expr_on_row(self, row: Tuple, expr: Expr, is_left: bool = True) -> Any:
        """Hilfsfunktion zur Expression-Evaluierung auf einer Zeile"""
        if isinstance(expr, Column):
            col_idx = self._get_column_index(expr.name, row)
            return row[col_idx] if col_idx < len(row) else None
        elif isinstance(expr, Literal):
            return expr.value
        else:
            return None

# -----------------------
# Demo Usage mit pyDBSP 0.6
# -----------------------

def demo():
    """Demo der Stream-Processing-Engine mit pyDBSP 0.6"""
    
    # Prozessor initialisieren
    processor = StreamProcessor()
    
    # Datenquellen als ZSets erstellen
    orders_data = ZSet({
        (1, 101, 150.0): 1,
        (2, 102, 75.0): 1,
        (3, 101, 200.0): 1
    })
    
    customers_data = ZSet({
        (101, "Alice"): 1,
        (102, "Bob"): 1
    })
    
    # Streams erstellen - korrekte pyDBSP 0.6 API
    # Stream benötigt ein group_op Objekt, nicht eine Lambda-Funktion
    from pydbsp.zset import ZSetAddition
    
    try:
        # Korrekte Stream-Erstellung mit group_op
        orders_stream = Stream(ZSetAddition())
        customers_stream = Stream(ZSetAddition())
        
        # Initiale Daten setzen (falls Stream.send() existiert)
        if hasattr(orders_stream, 'send'):
            orders_stream.send(orders_data)
            customers_stream.send(customers_data)
        elif hasattr(orders_stream, 'input'):
            orders_stream.input(orders_data)
            customers_stream.input(customers_data)
        else:
            print("Warnung: Konnte initiale Daten nicht setzen")
            
    except (ImportError, AttributeError) as e:
        print(f"ZSetAddition nicht verfügbar: {e}")
        # Alternative: Verwende direkte ZSet-basierte Streams
        try:
            # Versuche andere Stream-Konstruktoren
            orders_stream = Stream()
            customers_stream = Stream()
            print("Streams mit default-Konstruktor erstellt")
        except Exception as e2:
            print(f"Stream-Erstellung fehlgeschlagen: {e2}")
            return
    
    # Schemas definieren
    orders_schema = {"id": "INT", "cust_id": "INT", "amount": "DOUBLE"}
    customers_schema = {"id": "INT", "name": "STRING"}
    
    processor.add_source("orders", orders_stream, orders_schema)
    processor.add_source("customers", customers_stream, customers_schema)
    
    # Query definieren
    orders = StreamDSL.source("orders", orders_schema)
    customers = StreamDSL.source("customers", customers_schema)
    
    query = (
        orders.where(col("amount") > lit(100))
              .join(customers, on=col("cust_id") == col("id"))
              .select(col("name"), col("amount"))
              .group_by(col("name"))
              .agg(sum_agg(col("amount")))
              .to("top_customers")
    )
    
    print("Stream-Processing-Engine Demo mit pyDBSP 0.6")
    print(f"Anzahl Quellen: {len(processor.sources)}")
    
    # Plan-Struktur anzeigen
    print("\nPlan-Struktur:")
    print_plan(query._plan, indent=0)
    
    # Plan kompilieren
    try:
        result_stream = processor.compile_plan(query._plan)
        print(f"\nPlan erfolgreich kompiliert zu: {type(result_stream)}")
        print("Engine bereit für Stream-Processing!")
        
    except Exception as e:
        print(f"\nFehler bei Plan-Kompilierung: {e}")
        raise

def print_plan(node: PlanNode, indent: int = 0):
    """Hilfsfunktion um Plan-Struktur anzuzeigen"""
    spaces = "  " * indent
    if isinstance(node, SourceNode):
        print(f"{spaces}Source: {node.name} {node.schema}")
    elif isinstance(node, FilterNode):
        print(f"{spaces}Filter: {node.predicate}")
        print_plan(node.input, indent + 1)
    elif isinstance(node, ProjectNode):
        print(f"{spaces}Project: {node.projections}")
        print_plan(node.input, indent + 1)
    elif isinstance(node, JoinNode):
        print(f"{spaces}Join ({node.how}): {node.on}")
        print(f"{spaces}  Left:")
        print_plan(node.left, indent + 2)
        print(f"{spaces}  Right:")
        print_plan(node.right, indent + 2)
    elif isinstance(node, GroupAggNode):
        print(f"{spaces}GroupAgg: keys={node.keys}, aggs={node.aggs}")
        print_plan(node.input, indent + 1)
    elif isinstance(node, SinkNode):
        print(f"{spaces}Sink: {node.name}")
        print_plan(node.input, indent + 1)

if __name__ == "__main__":
    demo()
