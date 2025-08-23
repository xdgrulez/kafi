"""
PyDBSP-based Stream Processing Library with SQL Fluent API using Z-Sets
Note: This is a simplified implementation working with basic PyDBSP concepts
"""

from typing import Dict, List, Any, Optional, Callable, Union, Tuple
import re
from dataclasses import dataclass
from collections import defaultdict
from pydbsp.zset import ZSet

@dataclass(frozen=True)
class Record:
    """A database record - must be hashable for Z-Set"""
    data: tuple  # Use tuple instead of dict for hashability
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'Record':
        return cls(tuple(sorted(d.items())))
    
    def to_dict(self) -> Dict[str, Any]:
        return dict(self.data)
    
    def get(self, key: str, default=None):
        d = self.to_dict()
        return d.get(key, default)

class StreamOperator:
    """Base operator class for stream transformations"""
    def __init__(self, transform_fn: Callable[[ZSet], ZSet]):
        self.transform_fn = transform_fn
        self.input_data = None
        self.output_data = None
        
    def process(self, input_zset: ZSet) -> ZSet:
        """Apply transformation to input Z-Set"""
        return self.transform_fn(input_zset)
        
    def connect(self, upstream_op: 'StreamOperator') -> 'StreamOperator':
        """Connect this operator to an upstream operator"""
        def combined_transform(zset: ZSet) -> ZSet:
            intermediate = upstream_op.transform_fn(zset)
            return self.transform_fn(intermediate)
        return StreamOperator(combined_transform)

class SQLStreamBuilder:
    """Fluent API builder for SQL-based stream processing using Z-Sets"""
    
    def __init__(self):
        self.current_operator = None
        self.operations = []
        self.tables = {}
        
    def from_table(self, name: str, input_operator: StreamOperator) -> 'SQLStreamBuilder':
        """Start query from a table (StreamOperator)"""
        self.tables[name] = input_operator
        self.current_operator = input_operator
        self.operations.append(('FROM', name))
        return self
        
    def select(self, *columns) -> 'SQLStreamBuilder':
        """SELECT columns - projects records in Z-Set"""
        if not columns:
            columns = ['*']
        self.operations.append(('SELECT', list(columns)))
        
        def project_zset(zset: ZSet) -> ZSet:
            result_items = []
            for record, weight in zset.iter():
                record_dict = record.to_dict()
                if '*' in columns:
                    projected_data = record_dict
                else:
                    projected_data = {col: record_dict.get(col) for col in columns if col in record_dict}
                projected_record = Record.from_dict(projected_data)
                result_items.append((projected_record, weight))
            return ZSet.from_iter(result_items)
        
        new_op = StreamOperator(project_zset)
        self.current_operator = self.current_operator.connect(new_op)
        return self
        
    def where(self, condition: str) -> 'SQLStreamBuilder':
        """WHERE condition - filters records in Z-Set"""
        self.operations.append(('WHERE', condition))
        
        def filter_zset(zset: ZSet) -> ZSet:
            result_items = []
            for record, weight in zset.iter():
                if self._evaluate_condition(record, condition):
                    result_items.append((record, weight))
            return ZSet.from_iter(result_items)
        
        new_op = StreamOperator(filter_zset)
        self.current_operator = self.current_operator.connect(new_op)
        return self
        
    def _evaluate_condition(self, record: Record, condition: str) -> bool:
        """Evaluate WHERE condition"""
        record_dict = record.to_dict()
        expr = condition
        for key, value in record_dict.items():
            if isinstance(value, str):
                expr = expr.replace(key, f"'{value}'")
            else:
                expr = expr.replace(key, str(value))
        try:
            return eval(expr)
        except:
            return False
        
    def group_by(self, *columns) -> 'SQLStreamBuilder':
        """GROUP BY columns using Z-Set grouping"""
        self.operations.append(('GROUP BY', list(columns)))
        self.group_columns = columns
        
        def group_zset(zset: ZSet) -> ZSet:
            groups = defaultdict(list)
            
            # Group records by key
            for record, weight in zset.iter():
                record_dict = record.to_dict()
                key = tuple(record_dict.get(col) for col in columns)
                groups[key].append((record, weight))
            
            # Create group representatives
            result_items = []
            for key, records_with_weights in groups.items():
                # Create a record representing the group
                group_data = {col: key[i] for i, col in enumerate(columns)}
                # Store the grouped records for aggregation
                group_data['_grouped_records'] = records_with_weights
                group_record = Record.from_dict(group_data)
                result_items.append((group_record, 1))
                
            return ZSet.from_iter(result_items)
        
        new_op = StreamOperator(group_zset)
        self.current_operator = self.current_operator.connect(new_op)
        return self
        
    def aggregate(self, agg_funcs: Dict[str, str]) -> 'SQLStreamBuilder':
        """Aggregate functions using Z-Set operations"""
        self.operations.append(('AGGREGATE', agg_funcs))
        
        def aggregate_zset(zset: ZSet) -> ZSet:
            result_items = []
            
            for group_record, group_weight in zset.iter():
                group_dict = group_record.to_dict()
                if '_grouped_records' in group_dict:
                    records_with_weights = group_dict['_grouped_records']
                    agg_data = {k: v for k, v in group_dict.items() if k != '_grouped_records'}
                    
                    # Apply aggregation functions
                    for alias, func_expr in agg_funcs.items():
                        if func_expr.startswith('COUNT'):
                            # COUNT sums all weights in the group
                            count = sum(weight for _, weight in records_with_weights)
                            agg_data[alias] = count
                        elif func_expr.startswith('SUM'):
                            col = func_expr[4:-1]  # Extract column from SUM(column)
                            total = sum(
                                record.get(col, 0) * weight 
                                for record, weight in records_with_weights
                            )
                            agg_data[alias] = total
                        elif func_expr.startswith('AVG'):
                            col = func_expr[4:-1]
                            total_sum = sum(
                                record.get(col, 0) * weight 
                                for record, weight in records_with_weights
                            )
                            total_count = sum(weight for _, weight in records_with_weights)
                            agg_data[alias] = total_sum / total_count if total_count > 0 else 0
                        elif func_expr.startswith('MAX'):
                            col = func_expr[4:-1]
                            values = [record.get(col, 0) for record, weight in records_with_weights if weight > 0]
                            agg_data[alias] = max(values) if values else 0
                        elif func_expr.startswith('MIN'):
                            col = func_expr[4:-1]
                            values = [record.get(col, 0) for record, weight in records_with_weights if weight > 0]
                            agg_data[alias] = min(values) if values else 0
                    
                    agg_record = Record.from_dict(agg_data)
                    result_items.append((agg_record, group_weight))
                    
            return ZSet.from_iter(result_items)
        
        new_op = StreamOperator(aggregate_zset)
        self.current_operator = self.current_operator.connect(new_op)
        return self
        
    def having(self, condition: str) -> 'SQLStreamBuilder':
        """HAVING condition (after GROUP BY)"""
        self.operations.append(('HAVING', condition))
        
        def having_filter(zset: ZSet) -> ZSet:
            result_items = []
            for record, weight in zset.iter():
                if self._evaluate_condition(record, condition):
                    result_items.append((record, weight))
            return ZSet.from_iter(result_items)
        
        new_op = StreamOperator(having_filter)
        self.current_operator = self.current_operator.connect(new_op)
        return self
        
    def union(self, other_builder: 'SQLStreamBuilder') -> 'SQLStreamBuilder':
        """UNION - combines Z-Sets (adds weights)"""
        self.operations.append(('UNION', 'UNION'))
        
        def union_zsets(zset: ZSet) -> ZSet:
            # This is a simplified union - in real DBSP would need both inputs
            return zset
        
        new_op = StreamOperator(union_zsets)
        self.current_operator = self.current_operator.connect(new_op)
        return self
        
    def distinct(self) -> 'SQLStreamBuilder':
        """DISTINCT - keeps only records with positive weight"""
        self.operations.append(('DISTINCT', 'DISTINCT'))
        
        def distinct_zset(zset: ZSet) -> ZSet:
            result_items = []
            for record, weight in zset.iter():
                if weight > 0:
                    result_items.append((record, 1))  # Normalize to weight 1
            return ZSet.from_iter(result_items)
        
        new_op = StreamOperator(distinct_zset)
        self.current_operator = self.current_operator.connect(new_op)
        return self
        
    def build(self) -> StreamOperator:
        """Build the final stream operator"""
        return self.current_operator
        
    def visualize(self) -> str:
        """Generate ASCII art visualization of the query plan"""
        lines = []
        lines.append("┌─────────────────────────┐")
        lines.append("│   Z-Set Query Plan      │")
        lines.append("├─────────────────────────┤")
        
        for i, (op_type, op_detail) in enumerate(self.operations):
            if i > 0:
                lines.append("│            │            │")
                lines.append("│            ▼            │")
                
            if op_type == "FROM":
                lines.append(f"│  📊 FROM {op_detail:<13} │")
            elif op_type == "SELECT":
                cols = ", ".join(op_detail) if len(str(op_detail)) < 11 else str(op_detail)[:11] + "..."
                lines.append(f"│  🔍 SELECT {cols:<11} │")
            elif op_type == "WHERE":
                cond = op_detail[:11] + "..." if len(op_detail) > 11 else op_detail
                lines.append(f"│  ⚡ WHERE {cond:<12} │")
            elif op_type == "GROUP BY":
                cols = ", ".join(op_detail) if len(str(op_detail)) < 8 else str(op_detail)[:8] + "..."
                lines.append(f"│  📊 GROUP BY {cols:<8} │")
            elif op_type == "AGGREGATE":
                lines.append(f"│  🔢 AGGREGATE          │")
            elif op_type == "UNION":
                lines.append(f"│  ➕ UNION              │")
            elif op_type == "DISTINCT":
                lines.append(f"│  🎯 DISTINCT           │")
            else:
                lines.append(f"│  {op_type:<19} │")
                
        lines.append("│                         │")
        lines.append("│  📈 Output Z-Set        │")
        lines.append("└─────────────────────────┘")
        return "\n".join(lines)

class StreamProcessor:
    """Main stream processor using Z-Set concepts"""
    
    def __init__(self):
        self.operators = {}
        
    def create_input_operator(self, name: str) -> StreamOperator:
        """Create an input operator that can accept Z-Sets"""
        def identity_transform(zset: ZSet) -> ZSet:
            return zset
        
        operator = StreamOperator(identity_transform)
        self.operators[name] = operator
        return operator
        
    def sql(self) -> SQLStreamBuilder:
        """Start building SQL query"""
        return SQLStreamBuilder()

# Examples with working implementation
def example_basic_operations():
    """Basic operations example"""
    processor = StreamProcessor()
    
    # Create input operator
    orders_op = processor.create_input_operator("orders")
    
    # Build query
    builder = (processor.sql()
              .from_table("orders", orders_op)
              .select("customer", "amount")
              .where("amount > 100"))
    
    result_op = builder.build()
    
    print("Basic Query Plan:")
    print(builder.visualize())
    
    # Create sample Z-Set
    records = [
        (Record.from_dict({"customer": "Alice", "amount": 150}), 1),
        (Record.from_dict({"customer": "Bob", "amount": 75}), 1),
        (Record.from_dict({"customer": "Charlie", "amount": 200}), 2)
    ]
    sample_zset = ZSet.from_iter(records)
    
    # Process data
    result_zset = result_op.process(sample_zset)
    
    print("\nInput Z-Set:")
    for record, weight in sample_zset.iter():
        print(f"  {record.to_dict()} (weight: {weight})")
    
    print("\nFiltered Result Z-Set:")
    for record, weight in result_zset.iter():
        print(f"  {record.to_dict()} (weight: {weight})")
    
    return result_op

def example_aggregation():
    """Aggregation example"""
    processor = StreamProcessor()
    
    sales_op = processor.create_input_operator("sales")
    
    builder = (processor.sql()
              .from_table("sales", sales_op)
              .group_by("region")
              .aggregate({
                  "total_sales": "SUM(amount)",
                  "order_count": "COUNT(*)"
              }))
    
    result_op = builder.build()
    
    print("\nAggregation Query Plan:")
    print(builder.visualize())
    
    # Sample data with weights demonstrating Z-Set semantics
    records = [
        (Record.from_dict({"region": "North", "amount": 100}), 2),  # 2 occurrences
        (Record.from_dict({"region": "North", "amount": 200}), 1),
        (Record.from_dict({"region": "South", "amount": 150}), 3),  # 3 occurrences
    ]
    sales_zset = ZSet.from_iter(records)
    
    result_zset = result_op.process(sales_zset)
    
    print("\nInput Sales Z-Set:")
    for record, weight in sales_zset.iter():
        print(f"  {record.to_dict()} (weight: {weight})")
    
    print("\nAggregated Result Z-Set:")
    for record, weight in result_zset.iter():
        print(f"  {record.to_dict()} (weight: {weight})")
    
    return result_op

def example_incremental_updates():
    """Example showing Z-Set incremental updates (INSERT/DELETE)"""
    processor = StreamProcessor()
    
    inventory_op = processor.create_input_operator("inventory")
    
    builder = (processor.sql()
              .from_table("inventory", inventory_op)
              .select("product", "quantity")
              .where("quantity > 0")
              .distinct())
    
    result_op = builder.build()
    
    print("\nIncremental Updates Example:")
    print(builder.visualize())
    
    # Initial batch with some products
    initial_batch = [
        (Record.from_dict({"product": "Widget", "quantity": 10}), 1),
        (Record.from_dict({"product": "Gadget", "quantity": 5}), 1),
        (Record.from_dict({"product": "Tool", "quantity": 0}), 1),  # Will be filtered out
    ]
    initial_zset = ZSet.from_iter(initial_batch)
    
    print("\nInitial Inventory Z-Set:")
    for record, weight in initial_zset.iter():
        print(f"  {record.to_dict()} (weight: {weight})")
    
    initial_result = result_op.process(initial_zset)
    print("\nFiltered Result (quantity > 0):")
    for record, weight in initial_result.iter():
        print(f"  {record.to_dict()} (weight: {weight})")
    
    # Update: Delete Widget, Update Gadget, Insert New Product
    update_batch = [
        (Record.from_dict({"product": "Widget", "quantity": 10}), -1),  # DELETE
        (Record.from_dict({"product": "Gadget", "quantity": 5}), -1),   # DELETE old
        (Record.from_dict({"product": "Gadget", "quantity": 8}), 1),    # INSERT new
        (Record.from_dict({"product": "NewItem", "quantity": 3}), 1),   # INSERT
    ]
    update_zset = ZSet.from_iter(update_batch)
    
    print("\nUpdate Z-Set (showing deletes as negative weights):")
    for record, weight in update_zset.iter():
        print(f"  {record.to_dict()} (weight: {weight})")
    
    update_result = result_op.process(update_zset)
    print("\nUpdate Result:")
    for record, weight in update_result.iter():
        print(f"  {record.to_dict()} (weight: {weight})")
    
    return result_op

if __name__ == "__main__":
    print("=== PyDBSP SQL Stream Processing Examples ===\n")
    
    example_basic_operations()
    example_aggregation()
    example_incremental_updates()
