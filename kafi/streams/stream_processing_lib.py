"""
PyDBSP-based Stream Processing Library with SQL Fluent API
Pure PyDBSP implementation - requires pydbsp package
"""

from typing import Dict, List, Any, Optional, Callable, Union, Tuple
from dataclasses import dataclass
from collections import defaultdict

# Core PyDBSP imports
from pydbsp.zset import ZSet, ZSetAddition
from pydbsp.stream import Stream
from pydbsp import *

@dataclass(frozen=True)
class Record:
    """A database record - hashable for Z-Set usage"""
    data: tuple  # Use tuple for hashability
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'Record':
        """Create Record from dictionary"""
        return cls(tuple(sorted(d.items())))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Record back to dictionary"""
        return dict(self.data)
    
    def get(self, key: str, default=None):
        """Get value from record"""
        return self.to_dict().get(key, default)

class SQLStreamBuilder:
    """Fluent API for SQL-based stream processing with Z-Sets"""
    
    def __init__(self):
        self.current_zset_stream = None
        self.operations = []
        self.tables = {}
        
    def from_stream(self, name: str, zset_stream) -> 'SQLStreamBuilder':
        """Start query from a Z-Set stream"""
        self.tables[name] = zset_stream
        self.current_zset_stream = zset_stream
        self.operations.append(('FROM', name))
        return self
        
    def select(self, *columns) -> 'SQLStreamBuilder':
        """SELECT - project columns from records"""
        if not columns:
            columns = ['*']
        self.operations.append(('SELECT', list(columns)))
        
        def project_records(zset: ZSet) -> ZSet:
            """Project selected columns from all records in Z-Set"""
            result_zset = ZSet()
            
            for record, weight in zset.items():
                record_dict = record.to_dict()
                
                if '*' in columns:
                    projected_data = record_dict
                else:
                    projected_data = {
                        col: record_dict.get(col) 
                        for col in columns 
                        if col in record_dict
                    }
                
                projected_record = Record.from_dict(projected_data)
                result_zset = result_zset.add(projected_record, weight)
            
            return result_zset
        
        # Apply projection transformation
        self.current_zset_stream = self.current_zset_stream.map(project_records)
        return self
        
    def where(self, condition: str) -> 'SQLStreamBuilder':
        """WHERE - filter records based on condition"""
        self.operations.append(('WHERE', condition))
        
        def filter_records(zset: ZSet) -> ZSet:
            """Filter records that match the condition"""
            result_zset = ZSet()
            
            for record, weight in zset.items():
                if self._evaluate_condition(record, condition):
                    result_zset = result_zset.add(record, weight)
            
            return result_zset
        
        self.current_zset_stream = self.current_zset_stream.map(filter_records)
        return self
        
    def _evaluate_condition(self, record: Record, condition: str) -> bool:
        """Evaluate WHERE condition on a record"""
        record_dict = record.to_dict()
        expr = condition
        
        # Simple variable substitution
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
        """GROUP BY - group records by specified columns"""
        self.operations.append(('GROUP BY', list(columns)))
        self.group_columns = columns
        
        def group_records(zset: ZSet) -> ZSet:
            """Group records by key columns"""
            groups = defaultdict(list)
            
            # Collect records by group key
            for record, weight in zset.items():
                record_dict = record.to_dict()
                group_key = tuple(record_dict.get(col) for col in columns)
                groups[group_key].append((record, weight))
            
            # Create group representative records
            result_zset = ZSet()
            for group_key, records_weights in groups.items():
                # Create group metadata record
                group_data = {col: group_key[i] for i, col in enumerate(columns)}
                group_data['_group_records'] = records_weights  # Store for aggregation
                
                group_record = Record.from_dict(group_data)
                result_zset = result_zset.add(group_record, 1)
            
            return result_zset
        
        self.current_zset_stream = self.current_zset_stream.map(group_records)
        return self
        
    def aggregate(self, agg_functions: Dict[str, str]) -> 'SQLStreamBuilder':
        """Apply aggregation functions (COUNT, SUM, AVG, MIN, MAX)"""
        self.operations.append(('AGGREGATE', agg_functions))
        
        def apply_aggregations(zset: ZSet) -> ZSet:
            """Apply aggregation functions to grouped data"""
            result_zset = ZSet()
            
            for group_record, group_weight in zset.items():
                group_dict = group_record.to_dict()
                
                if '_group_records' in group_dict:
                    records_weights = group_dict['_group_records']
                    
                    # Start with group key columns
                    result_data = {
                        k: v for k, v in group_dict.items() 
                        if k != '_group_records'
                    }
                    
                    # Apply each aggregation function
                    for alias, func_expr in agg_functions.items():
                        if func_expr.startswith('COUNT'):
                            # COUNT(*) sums all weights in group
                            result_data[alias] = sum(
                                weight for _, weight in records_weights
                            )
                            
                        elif func_expr.startswith('SUM'):
                            # SUM(column) - weighted sum
                            col = func_expr[4:-1]  # Extract column name
                            result_data[alias] = sum(
                                record.get(col, 0) * weight 
                                for record, weight in records_weights
                            )
                            
                        elif func_expr.startswith('AVG'):
                            # AVG(column) - weighted average
                            col = func_expr[4:-1]
                            total_sum = sum(
                                record.get(col, 0) * weight 
                                for record, weight in records_weights
                            )
                            total_count = sum(weight for _, weight in records_weights)
                            result_data[alias] = (
                                total_sum / total_count if total_count > 0 else 0
                            )
                            
                        elif func_expr.startswith('MIN'):
                            col = func_expr[4:-1]
                            values = [
                                record.get(col, 0) 
                                for record, weight in records_weights 
                                if weight > 0
                            ]
                            result_data[alias] = min(values) if values else 0
                            
                        elif func_expr.startswith('MAX'):
                            col = func_expr[4:-1]
                            values = [
                                record.get(col, 0) 
                                for record, weight in records_weights 
                                if weight > 0
                            ]
                            result_data[alias] = max(values) if values else 0
                    
                    agg_record = Record.from_dict(result_data)
                    result_zset = result_zset.add(agg_record, group_weight)
            
            return result_zset
        
        self.current_zset_stream = self.current_zset_stream.map(apply_aggregations)
        return self
        
    def having(self, condition: str) -> 'SQLStreamBuilder':
        """HAVING - filter aggregated results"""
        self.operations.append(('HAVING', condition))
        
        def filter_aggregated(zset: ZSet) -> ZSet:
            """Filter aggregated records by HAVING condition"""
            result_zset = ZSet()
            
            for record, weight in zset.items():
                if self._evaluate_condition(record, condition):
                    result_zset = result_zset.add(record, weight)
            
            return result_zset
        
        self.current_zset_stream = self.current_zset_stream.map(filter_aggregated)
        return self
        
    def distinct(self) -> 'SQLStreamBuilder':
        """DISTINCT - remove duplicates (normalize weights to 0 or 1)"""
        self.operations.append(('DISTINCT', 'DISTINCT'))
        
        def make_distinct(zset: ZSet) -> ZSet:
            """Keep only records with positive weights, normalize to 1"""
            result_zset = ZSet()
            
            for record, weight in zset.items():
                if weight > 0:
                    result_zset = result_zset.add(record, 1)
            
            return result_zset
        
        self.current_zset_stream = self.current_zset_stream.map(make_distinct)
        return self
        
    def build(self):
        """Build and return the final Z-Set stream"""
        return self.current_zset_stream
        
    def visualize(self) -> str:
        """Generate ASCII visualization of query plan"""
        lines = []
        lines.append("┌─────────────────────────┐")
        lines.append("│    PyDBSP Query Plan    │")
        lines.append("├─────────────────────────┤")
        
        for i, (op_type, op_detail) in enumerate(self.operations):
            if i > 0:
                lines.append("│            │            │")
                lines.append("│            ▼            │")
                
            if op_type == "FROM":
                lines.append(f"│  📊 FROM {op_detail:<13} │")
            elif op_type == "SELECT":
                cols = ", ".join(op_detail) if len(str(op_detail)) < 11 else "..."
                lines.append(f"│  🔍 SELECT {cols:<11} │")
            elif op_type == "WHERE":
                cond = op_detail[:11] + "..." if len(op_detail) > 11 else op_detail
                lines.append(f"│  ⚡ WHERE {cond:<12} │")
            elif op_type == "GROUP BY":
                cols = ", ".join(op_detail) if len(str(op_detail)) < 8 else "..."
                lines.append(f"│  📊 GROUP BY {cols:<8} │")
            elif op_type == "AGGREGATE":
                lines.append(f"│  🔢 AGGREGATE          │")
            elif op_type == "HAVING":
                cond = op_detail[:11] + "..." if len(op_detail) > 11 else op_detail
                lines.append(f"│  🎯 HAVING {cond:<11} │")
            elif op_type == "DISTINCT":
                lines.append(f"│  ✨ DISTINCT           │")
            else:
                lines.append(f"│  {op_type:<19} │")
                
        lines.append("│                         │")
        lines.append("│  📈 Output Z-Set        │")
        lines.append("└─────────────────────────┘")
        return "\n".join(lines)

class StreamProcessor:
    """Main PyDBSP Stream Processor"""
    
    def __init__(self):
        self.streams = {}
        
    def create_input_stream(self, name: str):
        """Create input stream for Z-Sets"""
        # Using PyDBSP's stream creation
        input_stream = Stream(ZSetAddition())
        self.streams[name] = input_stream
        return input_stream
        
    def sql(self) -> SQLStreamBuilder:
        """Create new SQL query builder"""
        return SQLStreamBuilder()
        
    def push_zset(self, stream, zset: ZSet):
        """Push Z-Set to input stream"""
        stream.push(zset)

# Examples demonstrating PyDBSP Z-Set processing
def example_basic_sql():
    """Basic SELECT + WHERE example"""
    processor = StreamProcessor()
    
    # Create input stream
    orders_stream = processor.create_input_stream("orders")
    
    # Build SQL query
    query = (processor.sql()
            .from_stream("orders", orders_stream)
            .select("customer", "amount")
            .where("amount > 100")
            .distinct())
    
    result_stream = query.build()
    
    print("Basic SQL Query Plan:")
    print(query.visualize())
    
    # Sample data as Z-Set
    sample_data = ZSet()
    sample_data = sample_data.add(Record.from_dict({"customer": "Alice", "amount": 150}), 1)
    sample_data = sample_data.add(Record.from_dict({"customer": "Bob", "amount": 75}), 1)     # Filtered out
    sample_data = sample_data.add(Record.from_dict({"customer": "Charlie", "amount": 200}), 2)  # Weight 2
    
    # Process
    processor.push_zset(orders_stream, sample_data)
    
    print("\nInput Z-Set:")
    for record, weight in sample_data.items():
        print(f"  {record.to_dict()} -> weight: {weight}")
    
    return result_stream

def example_aggregation_sql():
    """GROUP BY + Aggregation example"""
    processor = StreamProcessor()
    
    sales_stream = processor.create_input_stream("sales")
    
    query = (processor.sql()
            .from_stream("sales", sales_stream)
            .group_by("region")
            .aggregate({
                "total_amount": "SUM(amount)",
                "order_count": "COUNT(*)",
                "avg_amount": "AVG(amount)"
            })
            .having("total_amount > 200"))
    
    result_stream = query.build()
    
    print("\nAggregation Query Plan:")
    print(query.visualize())
    
    # Sample sales data with Z-Set weights
    sales_data = ZSet()
    sales_data = sales_data.add(Record.from_dict({"region": "North", "amount": 100}), 2)  # 2 orders
    sales_data = sales_data.add(Record.from_dict({"region": "North", "amount": 50}), 1)   # 1 order
    sales_data = sales_data.add(Record.from_dict({"region": "South", "amount": 200}), 1)  # 1 order
    sales_data = sales_data.add(Record.from_dict({"region": "East", "amount": 75}), 3)     # 3 orders
    
    processor.push_zset(sales_stream, sales_data)
    
    print("\nSales Input Z-Set:")
    for record, weight in sales_data.items():
        print(f"  {record.to_dict()} -> weight: {weight}")
    
    return result_stream

def example_incremental_updates():
    """Demonstrate Z-Set incremental updates"""
    processor = StreamProcessor()
    
    inventory_stream = processor.create_input_stream("inventory")
    
    query = (processor.sql()
            .from_stream("inventory", inventory_stream)
            .select("product", "stock")
            .where("stock > 0"))
    
    result_stream = query.build()
    
    print("\nIncremental Updates Example:")
    print(query.visualize())
    
    # Initial state
    initial_state = ZSet()
    initial_state = initial_state.add(Record.from_dict({"product": "Widget", "stock": 10}), 1)
    initial_state = initial_state.add(Record.from_dict({"product": "Gadget", "stock": 5}), 1)
    initial_state = initial_state.add(Record.from_dict({"product": "Tool", "stock": 0}), 1)  # Filtered out
    
    print("\nInitial Inventory:")
    for record, weight in initial_state.items():
        print(f"  {record.to_dict()} -> weight: {weight}")
    
    # Updates: DELETE + INSERT operations using negative weights
    updates = ZSet()
    updates = updates.add(Record.from_dict({"product": "Widget", "stock": 10}), -1)  # DELETE
    updates = updates.add(Record.from_dict({"product": "Widget", "stock": 15}), 1)   # INSERT (update)
    updates = updates.add(Record.from_dict({"product": "NewProduct", "stock": 8}), 1) # INSERT
    updates = updates.add(Record.from_dict({"product": "Tool", "stock": 0}), -1)     # DELETE
    updates = updates.add(Record.from_dict({"product": "Tool", "stock": 3}), 1)      # INSERT (now in stock)
    
    print("\nUpdate Operations (with negative weights for deletes):")
    for record, weight in updates.items():
        op = "DELETE" if weight < 0 else "INSERT"
        print(f"  {op}: {record.to_dict()} -> weight: {weight}")
    
    # Apply updates
    processor.push_zset(inventory_stream, initial_state)
    processor.push_zset(inventory_stream, updates)
    
    return result_stream

if __name__ == "__main__":
    print("=== PyDBSP SQL Stream Processing Library ===\n")
    
    example_basic_sql()
    example_aggregation_sql()
    example_incremental_updates()
    
    print("\n=== Library Features ===")
    print("✅ Pure PyDBSP Z-Set processing")
    print("✅ SQL Fluent API (SELECT, WHERE, GROUP BY, etc.)")
    print("✅ Incremental updates with INSERT/DELETE semantics")
    print("✅ ASCII query plan visualization")
    print("✅ Weighted aggregations (COUNT, SUM, AVG, MIN, MAX)")
