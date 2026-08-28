import random

from streams.datagen.constants.product_ids import product_id_str_list
from streams.datagen.constants.customer_ids import customer_id_str_list

#

class ShoeOrderDebeziumGenerator:
    def __init__(self):
        self.order_id_int = 1000
        self.order_id_step_int = 1
        #
        self.ts_int = 1609459200000
        self.ts_step_int = 100000
        #
        self.inner_v_r_list = []

    def generate_record(self):
        k_r = None
        #
        if random.randrange(10) <= 8 or not self.inner_v_r_list:
            inner_v_r = {
                "order_id": self.order_id_int,
                "product_id": random.choice(product_id_str_list),
                "customer_id": random.choice(customer_id_str_list),
                "ts": self.ts_int
            }
            #
            self.order_id_int += self.order_id_step_int
            self.ts_int += self.ts_step_int
            #
            self.inner_v_r_list.append(inner_v_r)
            #
            v_r = {"before": None, "after": inner_v_r, "op": "c"}
        else:
            v_r_int = random.randint(0, len(self.inner_v_r_list) - 1)
            #
            inner_v_r = self.inner_v_r_list.pop(v_r_int)
            #
            v_r = {"before": inner_v_r, "after": None, "op": "d"}
        #
        return (k_r, v_r)

if __name__ == "__main__":
    generator = ShoeOrderDebeziumGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())
