import random

#

import sys
sys.path.insert(1, ".")

#

from test_kafi_streams.datagen.constants.product_ids import product_id_str_list
from test_kafi_streams.datagen.constants.customer_ids import customer_id_str_list

#
# product_id_str_list =["1", "2"]
# customer_id_str_list = ["1"]
#customer_id_str_list = ["1", "2", "3"]
#

class ShoeOrderDebeziumGenerator:
    def __init__(self):
        self.order_id_int = 1000
        self.order_id_step_int = 1
        #
        self.ts_int = 1609459200000
        self.ts_step_int = 100000
        #
        self.inner_record_dict_list = []

    def generate_record(self):
        if random.randrange(10) <= 8 or not self.inner_record_dict_list:
            inner_record_dict = {
                "order_id": self.order_id_int,
                "product_id": random.choice(product_id_str_list),
                "customer_id": random.choice(customer_id_str_list),
                "ts": self.ts_int
            }
            #
            self.order_id_int += self.order_id_step_int
            self.ts_int += self.ts_step_int
            #
            self.inner_record_dict_list.append(inner_record_dict)
            #
            record_dict = {"before": None, "after": inner_record_dict, "op": "c"}
        else:
            record_dict_int = random.randint(0, len(self.inner_record_dict_list) - 1)
            #
            inner_record_dict = self.inner_record_dict_list.pop(record_dict_int)
            #
            record_dict = {"before": inner_record_dict, "after": None, "op": "d"}
        #
        return record_dict

if __name__ == "__main__":
    generator = ShoeOrderDebeziumGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())
