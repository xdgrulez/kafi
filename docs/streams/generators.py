import random, time

from faker import Faker

class ClickGenerator():
    def __init__(self, customers_int=100):
        self.customers_int = customers_int
        self.ts_int = int(time.time() * 1000)
        self.ts_step_int = 100
        self.customer_id_int = 0

    def generate(self, n=1):
        m_list = []
        for _ in range(n):
            m = {
                "key": None,
                "value": {"customer_id": random.randint(0, self.customers_int - 1),
                          "view_time": random.randint(10, 120),
                          "ts": self.ts_int},
            }
            #
            self.ts_int += self.ts_step_int
            #
            m_list.append(m)
        #
        return m_list

class CustomerGenerator:
    def __init__(self, customers_int=100):
        self.customers_int = customers_int
        self.customer_id_int = 0
        self.customer_id_int_name_str_dict = {}
        fake = Faker()
        for customer_id_int in range(self.customers_int):
            name_str = fake.name()
            self.customer_id_int_name_str_dict[customer_id_int] = name_str

    def generate(self, n=1):
        m_list = []
        for _ in range(n):
            customer_id_int = random.randint(0, self.customers_int - 1)
            #
            m = {
                "key": str(customer_id_int),
                "value": {"id": customer_id_int,
                        "name": self.customer_id_int_name_str_dict[customer_id_int]}
            }
            #
            m_list.append(m)
        #
        return m_list

# click_generator = ClickGenerator()
# for _ in range(3):
#     print(click_generator.generate())

# customer_generator = CustomerGenerator()
# for _ in range(3):
#     print(customer_generator.generate())
