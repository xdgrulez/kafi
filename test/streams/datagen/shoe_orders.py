import random

from streams.datagen.constants.product_ids import product_id_str_list
from streams.datagen.constants.customer_ids import customer_id_str_list

#

# import sys
# sys.path.insert(1, ".")

#

#
# product_id_str_list =["1", "2"]
# customer_id_str_list = ["1", "2", "3"]
#

ts_int = 1609459200000
ts_step_int = 100000

#

class ShoeOrderGenerator:
    def __init__(self):
        self.order_id_int = 1000
        self.order_id_step_int = 1
        #
        self.ts_int = ts_int
        self.ts_step_int = ts_step_int

    def generate_record(self):
        record_dict = {
            "order_id": self.order_id_int,
            "product_id": random.choice(product_id_str_list),
            "customer_id": random.choice(customer_id_str_list[:5]),
            "ts": self.ts_int
        }
        #
        self.order_id_int += self.order_id_step_int
        self.ts_int += self.ts_step_int
        #
        return record_dict

if __name__ == "__main__":
    generator = ShoeOrderGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())
