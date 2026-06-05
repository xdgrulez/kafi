import random

from datagen.shoe_clickstream import ShoeClickstreamGenerator
from datagen.shoe_customers import ShoeCustomerGenerator
from datagen.shoes import ShoeProductGenerator 
from datagen.shoe_orders import ShoeOrderGenerator
from datagen.shoe_orders_debezium import ShoeOrderDebeziumGenerator
from jamie.transactions import TransactionGenerator
from test_kafi_streams.wc.lines import LineGenerator

#

class TestGenerate:
    def init_generate(self, source_str):
        match source_str:
            case "shoe_clickstream":
                self.source_str_generator_dict[source_str] = ShoeClickstreamGenerator()
            case "shoe_customers":
                self.source_str_generator_dict[source_str] = ShoeCustomerGenerator()
            case "shoes":
                self.source_str_generator_dict[source_str] = ShoeProductGenerator()
            case "shoe_orders":
                self.source_str_generator_dict[source_str] = ShoeOrderGenerator()
            case "shoe_orders_debezium":
                self.source_str_generator_dict[source_str] = ShoeOrderDebeziumGenerator()
            #
            case "transactions": 
                self.source_str_generator_dict[source_str] = TransactionGenerator()
            #
            case "lines":
                self.source_str_generator_dict[source_str] = LineGenerator()
            case _:
                raise Exception(f"Source not supported: {source_str}")

    def generate(self, source_str, batch_size_int):
        value_any_list = []
        #
        generator = self.source_str_generator_dict[source_str]
        #
        partitions_int = 10
        partition_int_counter_int_dict = {partition_int: 0 for partition_int in range(partitions_int)}
        for _ in range(batch_size_int):
            record_dict = generator.generate_record()
            partition_int = random.randrange(partitions_int)
            value_any = {"key": None,
                         "value": record_dict,
                         "partition": partition_int,
                         "offset": partition_int_counter_int_dict[partition_int]}
            partition_int_counter_int_dict[partition_int] += 1
            value_any_list.append(value_any)
        #
        self.source_str_input_value_any_list_dict[source_str] += value_any_list
        #
        for x in value_any_list:
            print(x)
        #
        return value_any_list
