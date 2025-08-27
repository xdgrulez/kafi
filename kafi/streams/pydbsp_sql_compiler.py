"""
SQL-zu-pydbsp Compiler basierend auf Feldera's Architektur-Konzepten

Diese Implementierung adaptiert die Feldera SQL-zu-DBSP Compiler-Architektur
für die aktuelle pydbsp Version, ohne Apache Calcite zu benötigen.
"""

import ast
import re
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from dataclasses import dataclass
from enum import Enum
from pydbsp.stream import Stream
from pydbsp.zset import ZSet
from pydbsp.operators import LiftedH

# ============================================================================
# SQL AST Knoten (vereinfachte Version von Calcite's RelNode)
# ============================================================================

class RelNodeType(Enum):
    """SQL Relation Node Typen - ähnlich Apache Calcite"""
    TABLE_SCAN = "TableScan"
    PROJECT = "Project"
    FILTER = "Filter"
    AGGREGATE = "Aggregate"
    JOIN = "Join"
    DISTINCT = "Distinct"
    SORT = "Sort"
    UNION = "Union"

@dataclass
class RelNode:
    """Basis-Klasse für relationale Operatoren (ähnlich Calcite's RelNode)"""
    node_type: RelNodeType
    inputs: List['RelNode']
    row_type: List[str]  # Spalten-Namen
    
class TableScanNode(RelNode):
    def __init__(self, table_name: str, columns: List[str]):
        super().__init__(RelNodeType.TABLE_SCAN, [], columns)
        self.table_name = table_name

class ProjectNode(RelNode):
    def __init__(self, input_node: RelNode, projections: List[str]):
        super().__init__(RelNodeType.PROJECT, [input_node], projections)
        self.projections = projections

class FilterNode(RelNode):
    def __init__(self, input_node: RelNode, condition: str):
        super().__init__(RelNodeType.FILTER, [input_node], input_node.row_type)
        self.condition = condition

class DistinctNode(RelNode):
    def __init__(self, input_node: RelNode):
        super().__init__(RelNodeType.DISTINCT, [input_node], input_node.row_type)

class AggregateNode(RelNode):
    def __init__(self, input_node: RelNode, group_by: List[str], aggregates: List[Dict]):
        columns = group_by + [agg['alias'] for agg in aggregates]
        super().__init__(RelNodeType.AGGREGATE, [input_node], columns)
        self.group_by = group_by
        self.aggregates = aggregates

class JoinNode(RelNode):
    def __init__(self, left: RelNode, right: RelNode, condition: str, join_type: str = "INNER"):
        combined_columns = left.row_type + right.row_type
        super().__init__(RelNodeType.JOIN, [left, right], combined_columns)
        self.condition = condition
        self.join_type = join_type

# ============================================================================
# SQL Parser (vereinfacht - ersetzt Apache Calcite's Parser)
# ============================================================================

class SimpleSQLParser:
    """
    Vereinfachter SQL Parser - ersetzt Apache Calcite für grundlegende SQL Queries.
    Basiert auf den Feldera Compiler-Konzepten.
    """
    
    def __init__(self):
        self.tables: Dict[str, List[str]] = {}  # table_name -> columns
    
    def register_table(self, table_name: str, columns: List[str]):
        """Registriert eine Tabelle mit ihren Spalten"""
        self.tables[table_name] = columns
    
    def parse(self, sql: str) -> RelNode:
        """
        Parst SQL zu einem RelNode-Baum (ähnlich Calcite's SqlNode -> RelNode)
        """
        sql = sql.strip().rstrip(';')
        
        # Basis SELECT Pattern
        select_match = re.match(r'SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*?))?(?:\s+GROUP\s+BY\s+(.*?))?(?:\s+ORDER\s+BY\s+(.*?))?', sql, re.IGNORECASE | re.DOTALL)
        
        if not select_match:
            raise ValueError(f"Unsupported SQL: {sql}")
        
        select_clause, table_name, where_clause, group_by_clause, order_by_clause = select_match.groups()
        
        if table_name not in self.tables:
            raise ValueError(f"Unknown table: {table_name}")
        
        # Starte mit TableScan
        current_node = TableScanNode(table_name, self.tables[table_name])
        
        # WHERE Clause -> Filter
        if where_clause:
            current_node = FilterNode(current_node, where_clause.strip())
        
        # GROUP BY -> Aggregate
        if group_by_clause:
            group_cols = [col.strip() for col in group_by_clause.split(',')]
            # Vereinfachte Aggregat-Erkennung
            aggregates = self._parse_aggregates(select_clause)
            current_node = AggregateNode(current_node, group_cols, aggregates)
        
        # SELECT Clause -> Project (wenn nicht nur *)
        if select_clause.strip() != '*':
            # Check für DISTINCT
            if select_clause.upper().startswith('DISTINCT'):
                select_clause = select_clause[8:].strip()  # Remove 'DISTINCT'
                projections = self._parse_projections(select_clause)
                current_node = ProjectNode(current_node, projections)
                current_node = DistinctNode(current_node)
            else:
                projections = self._parse_projections(select_clause)
                if projections != current_node.row_type:
                    current_node = ProjectNode(current_node, projections)
        
        return current_node
    
    def _parse_projections(self, select_clause: str) -> List[str]:
        """Parst die SELECT Projektion"""
        if select_clause.strip() == '*':
            return []  # Wird später aufgelöst
        return [col.strip() for col in select_clause.split(',')]
    
    def _parse_aggregates(self, select_clause: str) -> List[Dict]:
        """Parst Aggregat-Funktionen aus SELECT"""
        aggregates = []
        # Vereinfachte Regex für COUNT, SUM, AVG, etc.
        agg_pattern = r'(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*([^)]+)\s*\)(?:\s+AS\s+(\w+))?'
        matches = re.findall(agg_pattern, select_clause, re.IGNORECASE)
        
        for func, column, alias in matches:
            aggregates.append({
                'function': func.upper(),
                'column': column.strip(),
                'alias': alias if alias else f"{func.lower()}_{column}"
            })
        
        return aggregates

# ============================================================================
# DBSP Circuit Builder (ähnlich Feldera's DBSPCircuit)
# ============================================================================

class PyDBSPCircuit:
    """
    Baut DBSP Circuits aus RelNode-Bäumen - ähnlich Feldera's CircuitBuilder
    """
    
    def __init__(self):
        self.input_streams: Dict[str, Stream] = {}
        self.operators: Dict[str, Any] = {}
    
    def add_input_stream(self, table_name: str, stream: Stream):
        """Registriert einen Input-Stream für eine Tabelle"""
        self.input_streams[table_name] = stream
    
    def compile(self, rel_node: RelNode) -> Stream:
        """
        Kompiliert RelNode zu DBSP Stream - Hauptfunktion des Compilers
        """
        return self._compile_node(rel_node)
    
    def _compile_node(self, node: RelNode) -> Stream:
        """Rekursive Kompilierung von RelNodes zu DBSP Operatoren"""
        
        if node.node_type == RelNodeType.TABLE_SCAN:
            return self._compile_table_scan(node)
        
        elif node.node_type == RelNodeType.PROJECT:
            return self._compile_project(node)
        
        elif node.node_type == RelNodeType.FILTER:
            return self._compile_filter(node)
        
        elif node.node_type == RelNodeType.DISTINCT:
            return self._compile_distinct(node)
        
        elif node.node_type == RelNodeType.AGGREGATE:
            return self._compile_aggregate(node)
        
        elif node.node_type == RelNodeType.JOIN:
            return self._compile_join(node)
        
        else:
            raise ValueError(f"Unsupported node type: {node.node_type}")
    
    def _compile_table_scan(self, node: TableScanNode) -> Stream:
        """TableScan -> Input Stream"""
        if node.table_name not in self.input_streams:
            raise ValueError(f"No input stream registered for table: {node.table_name}")
        return self.input_streams[node.table_name]
    
    def _compile_project(self, node: ProjectNode) -> Stream:
        """Project -> Map Operator"""
        input_stream = self._compile_node(node.inputs[0])
        
        # Erstelle Projektions-Mapping
        input_columns = node.inputs[0].row_type
        column_indices = {col: i for i, col in enumerate(input_columns)}
        
        projection_indices = []
        for proj in node.projections:
            if proj in column_indices:
                projection_indices.append(column_indices[proj])
            else:
                raise ValueError(f"Unknown column in projection: {proj}")
        
        def project_fn(zset: ZSet) -> ZSet:
            result = {}
            for tuple_data, weight in zset.items():
                projected = tuple(tuple_data[i] for i in projection_indices)
                if projected in result:
                    result[projected] += weight
                else:
                    result[projected] = weight
            return ZSet(result)
        
        return input_stream.map(project_fn)
    
    def _compile_filter(self, node: FilterNode) -> Stream:
        """Filter -> Filter Operator"""
        input_stream = self._compile_node(node.inputs[0])
        
        # Vereinfachte Bedingung (sollte durch echten Expression-Parser ersetzt werden)
        condition_fn = self._parse_condition(node.condition, node.inputs[0].row_type)
        
        def filter_fn(zset: ZSet) -> ZSet:
            result = {}
            for tuple_data, weight in zset.items():
                if condition_fn(tuple_data):
                    result[tuple_data] = weight
            return ZSet(result)
        
        return input_stream.map(filter_fn)
    
    def _compile_distinct(self, node: DistinctNode) -> Stream:
        """DISTINCT -> LiftedH Operator (wie in unserer vorherigen Implementierung)"""
        input_stream = self._compile_node(node.inputs[0])
        
        class DistinctState:
            def __init__(self):
                self.seen = set()
            def copy(self):
                new_state = DistinctState()
                new_state.seen = self.seen.copy()
                return new_state
        
        def distinct_transition(state: DistinctState, input_zset: ZSet) -> Tuple[DistinctState, ZSet]:
            new_state = state.copy()
            output_changes = {}
            
            for tuple_data, weight in input_zset.items():
                if weight > 0:  # Addition
                    if tuple_data not in new_state.seen:
                        new_state.seen.add(tuple_data)
                        output_changes[tuple_data] = 1
                elif weight < 0:  # Removal
                    if tuple_data in new_state.seen:
                        new_state.seen.discard(tuple_data)
                        output_changes[tuple_data] = -1
            
            return new_state, ZSet(output_changes)
        
        distinct_op = LiftedH(
            initial_state=DistinctState(),
            transition_function=distinct_transition
        )
        
        return distinct_op(input_stream)
    
    def _compile_aggregate(self, node: AggregateNode) -> Stream:
        """Aggregate -> LiftedH mit Gruppierung"""
        input_stream = self._compile_node(node.inputs[0])
        
        input_columns = node.inputs[0].row_type
        column_indices = {col: i for i, col in enumerate(input_columns)}
        
        group_indices = [column_indices[col] for col in node.group_by]
        
        class AggregateState:
            def __init__(self):
                self.groups = {}  # group_key -> {col: value}
            def copy(self):
                new_state = AggregateState()
                new_state.groups = {k: v.copy() for k, v in self.groups.items()}
                return new_state
        
        def aggregate_transition(state: AggregateState, input_zset: ZSet) -> Tuple[AggregateState, ZSet]:
            new_state = state.copy()
            output_changes = {}
            
            for tuple_data, weight in input_zset.items():
                group_key = tuple(tuple_data[i] for i in group_indices)
                
                if group_key not in new_state.groups:
                    new_state.groups[group_key] = {'count': 0, 'sum': {}, 'values': []}
                
                # Vereinfachte Aggregation (COUNT, SUM)
                group_data = new_state.groups[group_key]
                
                if weight > 0:
                    group_data['count'] += weight
                    group_data['values'].extend([tuple_data] * weight)
                elif weight < 0:
                    group_data['count'] -= abs(weight)
                    # Remove values (vereinfacht)
                
                # Berechne Aggregat-Ergebnisse
                agg_results = []
                for agg in node.aggregates:
                    if agg['function'] == 'COUNT':
                        agg_results.append(group_data['count'])
                    elif agg['function'] == 'SUM':
                        col_idx = column_indices[agg['column']]
                        total = sum(row[col_idx] for row in group_data['values'])
                        agg_results.append(total)
                
                result_tuple = group_key + tuple(agg_results)
                output_changes[result_tuple] = 1 if group_data['count'] > 0 else -1
            
            return new_state, ZSet(output_changes)
        
        agg_op = LiftedH(
            initial_state=AggregateState(),
            transition_function=aggregate_transition
        )
        
        return agg_op(input_stream)
    
    def _compile_join(self, node: JoinNode) -> Stream:
        """JOIN -> Join Operator (vereinfacht)"""
        left_stream = self._compile_node(node.inputs[0])
        right_stream = self._compile_node(node.inputs[1])
        
        # Vereinfachter Inner Join
        def join_fn(left_zset: ZSet, right_zset: ZSet) -> ZSet:
            result = {}
            for left_tuple, left_weight in left_zset.items():
                for right_tuple, right_weight in right_zset.items():
                    # Vereinfachte Join-Bedingung (sollte geparst werden)
                    joined_tuple = left_tuple + right_tuple
                    combined_weight = left_weight * right_weight
                    if combined_weight != 0:
                        if joined_tuple in result:
                            result[joined_tuple] += combined_weight
                        else:
                            result[joined_tuple] = combined_weight
            return ZSet(result)
        
        # Hier würde in echter Implementierung ein Binary Operator benötigt
        # Vereinfachung für Demo
        return left_stream  # Placeholder
    
    def _parse_condition(self, condition: str, columns: List[str]) -> Callable:
        """Parst WHERE-Bedingungen zu Python-Funktionen"""
        # Sehr vereinfachte Implementierung - sollte durch echten Parser ersetzt werden
        column_indices = {col: i for i, col in enumerate(columns)}
        
        # Einfache Patterns wie "age > 25"
        if '>' in condition:
            parts = condition.split('>')
            col_name = parts[0].strip()
            value = int(parts[1].strip())
            col_idx = column_indices[col_name]
            return lambda tuple_data: tuple_data[col_idx] > value
        
        elif '=' in condition:
            parts = condition.split('=')
            col_name = parts[0].strip()
            value = parts[1].strip().strip("'\"")
            col_idx = column_indices[col_name]
            return lambda tuple_data: tuple_data[col_idx] == value
        
        else:
            # Default: immer True
            return lambda tuple_data: True

# ============================================================================
# High-Level SQL Streaming Engine
# ============================================================================

class SQLStreamEngine:
    """
    High-Level Interface für SQL-basierte Stream-Verarbeitung
    Kombiniert Parser, Compiler und Circuit Builder
    """
    
    def __init__(self):
        self.parser = SimpleSQLParser()
        self.circuit = PyDBSPCircuit()
    
    def register_table(self, table_name: str, columns: List[str], stream: Stream):
        """Registriert eine Streaming-Tabelle"""
        self.parser.register_table(table_name, columns)
        self.circuit.add_input_stream(table_name, stream)
    
    def execute_sql(self, sql: str) -> Stream:
        """
        Führt SQL Query aus und gibt resultierenden Stream zurück
        """
        # 1. Parse SQL zu RelNode
        rel_tree = self.parser.parse(sql)
        
        # 2. Compile RelNode zu DBSP Circuit
        result_stream = self.circuit.compile(rel_tree)
        
        return result_stream
    
    def create_view(self, view_name: str, sql: str):
        """Erstellt eine materialisierte View"""
        result_stream = self.execute_sql(sql)
        # View könnte als neue Tabelle registriert werden
        return result_stream

# ============================================================================
# Beispiel-Verwendung
# ============================================================================

def example_usage():
    """Beispiel für die Verwendung des SQL Stream Engines"""
    from pydbsp.stream import Stream
    from pydbsp.zset import ZSet
    
    # Setup
    engine = SQLStreamEngine()
    
    # Test-Daten
    users_data = [
        ZSet({('Alice', 25, 'Engineer'): 1, ('Bob', 30, 'Manager'): 1}),
        ZSet({('Charlie', 35, 'Developer'): 1, ('Alice', 25, 'Engineer'): 1}),  # Alice duplicate
    ]
    
    users_stream = Stream(users_data)
    
    # Registriere Tabelle
    engine.register_table('users', ['name', 'age', 'role'], users_stream)
    
    # SQL Queries
    queries = [
        "SELECT * FROM users",
        "SELECT DISTINCT name FROM users",
        "SELECT name, age FROM users WHERE age > 25",
        "SELECT role, COUNT(*) FROM users GROUP BY role"
    ]
    
    for sql in queries:
        print(f"\nSQL: {sql}")
        try:
            result_stream = engine.execute_sql(sql)
            print(f"Result: {result_stream}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    example_usage()