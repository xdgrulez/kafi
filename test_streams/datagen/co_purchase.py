import random
from collections import defaultdict

from test_streams.datagen.constants.product_ids import product_id_str_list
from test_streams.datagen.constants.customer_ids import customer_id_str_list

# from constants.product_ids import product_id_str_list
# from constants.customer_ids import customer_id_str_list

class CoPurchaseGenerator:
    def __init__(self):
        self.ts_int = 1609457200000
        self.ts_step_int = 1000
        #
        self.power_customer_ratio_float = 0.05
        self.power_customer_multiplier_float = 5.0
        self.remove_probability_float = 0.2
        #
        self.customer_id_str_cart_list_dict = defaultdict(list)
        #
        customers_int = len(customer_id_str_list)
        power_customers_int = int(customers_int * self.power_customer_ratio_float)
        #
        self.power_customer_id_str_set = set(random.sample(customer_id_str_list, power_customers_int))


    def generate_record(self):
        customer_id_str = random.choice(customer_id_str_list)
        #
        if customer_id_str in self.power_customer_id_str_set:
            session_length_int = max(1, int(random.gauss(10 * self.power_customer_multiplier_float, 3)))
        else:
            session_length_int = max(1, int(random.gauss(10, 2)))
        #
        action_str_list = []
        for _ in range(session_length_int):
            if self.customer_id_str_cart_list_dict[customer_id_str] and random.random() < self.remove_probability_float:
                product_id_str = random.choice(list(self.customer_id_str_cart_list_dict[customer_id_str]))
                self.customer_id_str_cart_list_dict[customer_id_str] = list(filter(lambda x: x != product_id_str, self.customer_id_str_cart_list_dict[customer_id_str]))
                action_str = "remove"
            else:
                product_id_str = random.choice(product_id_str_list)
                self.customer_id_str_cart_list_dict[customer_id_str].append(product_id_str)
                action_str = "add"
            #
            action_str_list.append((product_id_str, action_str))
        #
        product_id_str, action_str = random.choice(action_str_list)
        #
        record_dict = {
            "user_id": customer_id_str,
            "product_id": product_id_str,
            "action": action_str,
            "ts": self.ts_int
        }
        #
        self.ts_int += self.ts_step_int
        #
        return record_dict


if __name__ == "__main__":
    generator = CoPurchaseGenerator()
    #
    for _ in range(10):
        print(generator.generate_record())
