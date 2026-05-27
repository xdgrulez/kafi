import random
from collections import defaultdict

# from test_streams.datagen.constants.product_ids import product_id_str_list
# from test_streams.datagen.constants.customer_ids import customer_id_str_list

# from constants.product_ids import product_id_str_list
# from constants.customer_ids import customer_id_str_list

product_id_str_list = [f"{i}" for i in range(3)]
customer_id_str_list = [f"{i}" for i in range(2)]

class CoPurchaseGenerator:
    def __init__(self):
        self.ts_int = 1609457200000
        self.ts_step_int = 1000
        #
        self.remove_probability_float = 0.5
        #
        self.customer_id_str_product_id_str_list_dict = defaultdict(set)

        self.counter = 0

    def generate_record(self):
        def create(user_id_int, product_id_int, price_int, action_str):
            return {"user_id": f"{user_id_int}",
                    "product_id": f"{product_id_int}",
                    "price": price_int,
                    "action": action_str}
        #
        match self.counter:
          case 0: record_dict = create(0, 0, 100, "add")
          case 1: record_dict = create(1, 0, 100, "add")
          case 2: record_dict = create(0, 1, 50, "add")
          case 3: record_dict = create(0, 0, 100, "remove")
        #
        self.counter += 1
        #
        print(record_dict)
        return record_dict

    def generate_record_1(self):
        customer_id_str = random.choice(customer_id_str_list)
        #
        if self.customer_id_str_product_id_str_list_dict[customer_id_str] and random.random() < self.remove_probability_float:
            product_id_str = random.choice(list(self.customer_id_str_product_id_str_list_dict[customer_id_str]))
            self.customer_id_str_product_id_str_list_dict[customer_id_str].remove(product_id_str)
            action_str = "remove"
        else:
            product_id_str = random.choice(product_id_str_list)
            self.customer_id_str_product_id_str_list_dict[customer_id_str].add(product_id_str)
            action_str = "add"
        #
        record_dict = {
            "user_id": customer_id_str,
            "product_id": product_id_str,
            "action": action_str,
            "ts": self.ts_int
        }
        print(record_dict)
        #
        self.ts_int += self.ts_step_int
        #
        return record_dict


if __name__ == "__main__":
    generator = CoPurchaseGenerator()
    #
    for _ in range(10):
        print(generator.generate_record())
