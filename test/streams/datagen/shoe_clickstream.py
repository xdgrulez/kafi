import random, time

from streams.datagen.constants.product_ids import product_id_str_list
from streams.datagen.constants.customer_ids import customer_id_str_list
from streams.datagen.constants.ips import ip_str_list

ts_int = 1609459200000
ts_step_int = 100000

#

class ShoeClickstreamGenerator:
    def __init__(self):
        self.ts_int = ts_int
        # self.ts_int = int(time.time() * 1000)
        self.ts_step_int = ts_step_int

    def generate_record(self):
        record_dict = {
            "product_id": random.choice(product_id_str_list),
            "user_id":    random.choice(customer_id_str_list),
            "view_time":  random.randint(10, 120),
            "ip":         random.choice(ip_str_list),
            "ts":         self.ts_int
        }
        #
        self.ts_int += self.ts_step_int
        #
        return record_dict

if __name__ == "__main__":
    generator = ShoeClickstreamGenerator()
    #
    for _ in range(3):
        print(generator.generate_record())
