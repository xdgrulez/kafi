from typing import NamedTuple

from pydbsp1 import (
    DeltaLiftedDeltaLiftedSortMergeJoin,
    DeltaLiftedDistinct,
    Program2D,
    Source,
)


class Order(NamedTuple):
    id: int
    customer: int
    total: float


class Customer(NamedTuple):
    id: int
    country: str


class EEOrder(NamedTuple):
    order_id: int
    customer_id: int
    total: float


p = Program2D(gc=True)
orders: Source[Order] = p.source("orders")
customers: Source[Customer] = p.source("customers")

ee_orders = DeltaLiftedDeltaLiftedSortMergeJoin(
    orders,
    customers,
    left_key=lambda o: o.customer,
    right_key=lambda c: c.id,
    projection=lambda o, c: EEOrder(o.id, c.id, o.total),
)

dis = DeltaLiftedDistinct(ee_orders)

view = p.view("ee_orders", dis)
p.insert(orders, [Order(1, 7, 30.0)])
p.insert(customers, [Customer(7, "EE")])
# p.step({orders: [Order(1, 7, 30.0)], customers: [Customer(7, "EE")]})
p.step()

print(view.delta().inner)
print(view.materialized().inner)
