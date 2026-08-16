import random, time, uuid

from faker import Faker

class ClickGenerator():
    def __init__(self, customers_int=100, ts_int=int(time.time() * 1000), ts_step_int=100):
        self.customers_int = customers_int
        self.customer_id_int = 0
        #
        self.ts_int = ts_int
        self.ts_step_int = ts_step_int

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

#

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

#

class OrderGenerator:
    def __init__(self, customers_int=10, ts_int=0, ts_step_int=1):
        self.customers_int = customers_int
        self.customer_id_int = 0
        #
        self.ts_int = ts_int
        self.ts_step_int = ts_step_int

    def generate(self, n=1):
        m_list = []
        for _ in range(n):
            order_id_str = str(uuid.uuid4())
            m = {
                "key": order_id_str,
                "value": {"order_id": order_id_str,
                        "customer_id": random.randint(0, self.customers_int - 1),
                        "price": random.randint(1, 10000) / 100,
                        "ts": self.ts_int},
            }
            #
            self.ts_int += self.ts_step_int
            #
            m_list.append(m)
        #
        return m
