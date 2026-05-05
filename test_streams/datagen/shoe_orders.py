import random

from test_streams.datagen.constants.product_ids import product_id_str_list
from test_streams.datagen.constants.customer_ids import customer_id_str_list

class ShoeOrderGenerator:
    def __init__(self):
        self.order_id_int = 1000
        self.order_id_step_int = 1
        #
        self.ts_int = 1609459200000
        self.ts_step_int = 100000

    def generate_record(self):
        record_dict = {
            "order_id": self.order_id_int,
            "product_id": random.choice(product_id_str_list),
            "customer_id": random.choice(customer_id_str_list),
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
