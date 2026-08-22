import copy, random, time, uuid

from faker import Faker

class ClickGenerator():
    def __init__(self, customers_int=100, ts_int=int(time.time() * 1000), ts_step_int=100, debezium_bool=False, weights_bool=False):
        self.customers_int = customers_int
        self.customer_id_int = 0
        #
        self.ts_int = ts_int
        self.ts_step_int = ts_step_int
        #
        self.debezium_bool = debezium_bool
        self.weights_bool = weights_bool

    def generate(self, n=1, w=1):
        m_or_m_w_tuple_list = []
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
            if self.debezium_bool:
                m = m_to_debezium(m, w)
                m_or_m_w_tuple_list.append(m)
            elif self.weights_bool:
                m_w_tuple = (m, w)
                m_or_m_w_tuple_list.append(m_w_tuple)
            else:
                m_or_m_w_tuple_list.append(m)
        #
        return m_or_m_w_tuple_list

#

class CustomerGenerator:
    def __init__(self, customers_int=100, debezium_bool=False, weights_bool=False):
        self.customers_int = customers_int
        self.customer_id_int = 0
        self.customer_id_int_name_str_dict = {}
        fake = Faker()
        for customer_id_int in range(self.customers_int):
            name_str = fake.name()
            self.customer_id_int_name_str_dict[customer_id_int] = name_str
        #
        self.debezium_bool = debezium_bool
        self.weights_bool = weights_bool

    def generate(self, n=1, w=1):
        m_or_m_w_tuple_list = []
        for _ in range(n):
            customer_id_int = random.randint(0, self.customers_int - 1)
            #
            m = {
                "key": str(customer_id_int),
                "value": {"id": customer_id_int,
                        "name": self.customer_id_int_name_str_dict[customer_id_int]}
            }
            #
            if self.debezium_bool:
                m = m_to_debezium(m, w)
                m_or_m_w_tuple_list.append(m)
            elif self.weights_bool:
                m_w_tuple = (m, w)
                m_or_m_w_tuple_list.append(m_w_tuple)
            else:
                m_or_m_w_tuple_list.append(m)
        #
        return m_or_m_w_tuple_list

#

class OrderGenerator:
    def __init__(self, customers_int=10, ts_int=0, ts_step_int=1, debezium_bool=False, weights_bool=False):
        self.customers_int = customers_int
        self.customer_id_int = 0
        #
        self.ts_int = ts_int
        self.ts_step_int = ts_step_int
        #
        self.debezium_bool = debezium_bool
        self.weights_bool = weights_bool

    def generate(self, n=1, w=1):
        m_or_m_w_tuple_list = []
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
            if self.debezium_bool:
                m = m_to_debezium(m, w)
                m_or_m_w_tuple_list.append(m)
            elif self.weights_bool:
                m_w_tuple = (m, w)
                m_or_m_w_tuple_list.append(m_w_tuple)
            else:
                m_or_m_w_tuple_list.append(m)
        #
        return m

#

def m_to_debezium(m, w):
    if w > 0:
        for _ in range(w):
            m1 = copy.deepcopy(m)
            m1["value"]["before"] = None
            m1["value"]["after"] = m["value"]
            m1["value"]["op"] = "c"
            return m1
    elif w < 0:
        for _ in range(-w):
            m1 = copy.deepcopy(m)
            m1["value"]["before"] = m["value"]
            m1["value"]["after"] = None
            m1["value"]["op"] = "d"
            return m1
