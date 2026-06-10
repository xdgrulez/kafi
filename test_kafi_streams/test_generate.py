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
        self.generate_offset_int = 0
        #
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
        record_any_list = []
        #
        generator = self.source_str_generator_dict[source_str]
        #
        for _ in range(batch_size_int):
            record_dict = generator.generate_record()
            record_any = {"key": None,
                         "value": record_dict,
                         "partition": 0,
                         "offset": self.generate_offset_int}
            record_any_list.append(record_any)
            self.generate_offset_int += 1
        #
        return record_any_list
