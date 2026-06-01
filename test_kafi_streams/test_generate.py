from datagen.shoe_clickstream import ShoeClickstreamGenerator
from datagen.shoe_customers import ShoeCustomerGenerator
from datagen.shoes import ShoeProductGenerator 
from datagen.shoe_orders import ShoeOrderGenerator
from jamie.transactions import TransactionGenerator
from test_kafi_streams.wc.lines import LineGenerator

#

class TestGenerate:
    def init_generate(self, source_str):
        match source_str:
            case "shoe_clickstream":
                self.generator_dict[source_str] = ShoeClickstreamGenerator()
            case "shoe_customers":
                self.generator_dict[source_str] = ShoeCustomerGenerator()
            case "shoes":
                self.generator_dict[source_str] = ShoeProductGenerator()
            case "shoe_orders":
                self.generator_dict[source_str] = ShoeOrderGenerator()
            #
            case "transactions": 
                self.generator_dict[source_str] = TransactionGenerator()
            #
            case "lines":
                self.generator_dict[source_str] = LineGenerator()
            case _:
                raise Exception(f"Source not supported: {source_str}")

    def generate(self, source_str, batch_size_int):
        value_any_list = []
        #
        generator = self.generator_dict[source_str]
        #
        for _ in range(batch_size_int):
            record_dict = generator.generate_record()
            value_any = {"key": None,
                         "value": record_dict}
            value_any_list.append(value_any)
        #
        return value_any_list
