import time

from kafi.streams.topologynode import source

#

def get_root_tn_datagen_1_join(click_source_str, customer_source_str):
    click_source_tn = source(click_source_str)
    customer_source_tn = source(customer_source_str)
    #
    click_tn = (
        click_source_tn
        .peek(lambda x, _: print((x["value"]["ts"], int(time.time() * 1000))))
        .peek(lambda x, _: print(x["value"]["ts"] - int(time.time() * 1000)))
        .select(lambda x: x["value"]["ts"] - int(time.time() * 1000) < 100000)
        .peek()
        .project(lambda x: {"user_id": x["value"]["user_id"], "ip": x["value"]["ip"]})
        .distinct()
    )
    #
    customer_tn = (
        customer_source_tn
        .project(lambda x: {"id": x["value"]["id"], "first_name": x["value"]["first_name"]})
        .distinct()
    )
    #
    root_tn = (
        click_tn
        .join_equi(
            customer_tn,
            lambda l: l["user_id"],
            lambda r: r["id"],
            lambda l, r: {"value": {
                "user_id": l["user_id"],
                "ip": l["ip"],
                "first_name": r["first_name"]}})
    )
    #
    root_tn.build()
    #
    return root_tn

def get_root_tn_datagen_2_joins(click_source_str, customer_source_str, product_source_str):
    click_source_tn = source(click_source_str)
    customer_source_tn = source(customer_source_str)
    product_source_tn = source(product_source_str)
    #
    click_tn = (
        click_source_tn
        .project(lambda x: {"user_id": x["value"]["user_id"], "ip": x["value"]["ip"], "product_id": x["value"]["product_id"]})
        .distinct()
    )
    #
    customer_tn = (
        customer_source_tn
        .project(lambda x: {"id": x["value"]["id"], "first_name": x["value"]["first_name"]})
        .distinct()
    )
    #
    product_tn = (
        product_source_tn
        .project(lambda x: {"id": x["value"]["id"], "brand": x["value"]["brand"]})
        .distinct()
    )
    #
    root_tn = (
        click_tn
        .join_equi(
            customer_tn,
            lambda l: l["user_id"],
            lambda r: r["id"],
            lambda l, r: {
                "user_id": l["user_id"],
                "ip": l["ip"],
                "product_id": l["product_id"],
                "first_name": r["first_name"]})
        .join_equi(
            product_tn,
            lambda l: l["product_id"],
            lambda r: r["id"],
            lambda l, r: {"value": {"user_id": l["user_id"],
                                    "ip": l["ip"],
                                    "product_id": l["product_id"],
                                    "first_name": l["first_name"],
                                    "brand": r["brand"]}})
    )
    #
    root_tn.build()
    #
    return root_tn

def get_root_tn_datagen_3_joins(click_source_str, customer_source_str, product_source_str, order_source_str):
    click_source_tn = source(click_source_str)
    customer_source_tn = source(customer_source_str)
    product_source_tn = source(product_source_str)
    order_source_tn = source(order_source_str)
    #
    click_tn = (
        click_source_tn
        .project(lambda x: {"user_id": x["value"]["user_id"], "ip": x["value"]["ip"], "product_id": x["value"]["product_id"]})
        .distinct()
    )
    #
    customer_tn = (
        customer_source_tn
        .project(lambda x: {"id": x["value"]["id"], "first_name": x["value"]["first_name"]})
        .distinct()
    )
    #
    product_tn = (
        product_source_tn
        .project(lambda x: {"id": x["value"]["id"], "brand": x["value"]["brand"]})
        .distinct()
    )
    #
    order_tn = (
        order_source_tn
        .project(lambda x: {"order_id": x["value"]["order_id"], "product_id": x["value"]["product_id"], "customer_id": x["value"]["customer_id"]})
        .distinct()
    )
    #
    root_tn = (
        click_tn
        .join_equi(
            order_tn,
            lambda l: (l["product_id"], l["user_id"]),
            lambda r: (r["product_id"], r["customer_id"]),
            lambda l, r: {
                "user_id": l["user_id"],
                "ip": l["ip"],
                "product_id": l["product_id"],
                "order_id": r["order_id"]})
        .join_equi(
            customer_tn,
            lambda l: l["user_id"],
            lambda r: r["id"],
            lambda l, r: {
                "user_id": l["user_id"],
                "ip": l["ip"],
                "product_id": l["product_id"],
                "order_id": l["order_id"],
                "first_name": r["first_name"]})
        .join_equi(
            product_tn,
            lambda l: l["product_id"],
            lambda r: r["id"],
            lambda l, r: {"value": {
                "user_id": l["user_id"],
                "ip": l["ip"],
                "product_id": l["product_id"],
                "first_name": l["first_name"],
                "brand": r["brand"],
                "order_id": l["order_id"]}})
    )
    #
    root_tn.build()
    #
    return root_tn

def get_root_tn_datagen_self_join_group_by(order_source_str):
    order_source_tn = source(order_source_str)
    #
    order_tn = (
        order_source_tn
        .from_value()
        .project(lambda x: {"product_id": x["product_id"], "customer_id": x["customer_id"]})
        .distinct()
    )
    #
    root_tn = (
        order_tn
        .join_equi(
            order_tn,
            lambda l: l["customer_id"],
            lambda r: r["customer_id"],
            lambda l, r: {"product_id_1": l["product_id"],
                          "product_id_2": r["product_id"],
                          "customer_id": l["customer_id"]}
        )
        .select(lambda x: x["product_id_1"] < x["product_id_2"])
        .distinct()
        .group_by_count(
            lambda x: {"product_id_1": x["product_id_1"], "product_id_2": x["product_id_2"]},
            lambda x, y: {"product_id_1": x["product_id_1"], "product_id_2": x["product_id_2"], "cross_purchases": y}
        )
        .to_value()
    )
    #
    root_tn.build()
    #
    return root_tn

def get_root_tn_datagen_self_join_group_by_debezium(order_source_str):
    order_source_tn = source(order_source_str)
    order_source_tn._to_zSet_function = order_source_tn.debezium_to_zSet
    #
    order_tn = (
        order_source_tn
        .from_value()
        .project(lambda x: {"product_id": x["product_id"], "customer_id": x["customer_id"]})
        .distinct()
    )
    #
    root_tn = (
        order_tn
        .join_equi(
            order_tn,
            lambda l: l["customer_id"],
            lambda r: r["customer_id"],
            lambda l, r: {"product_id_1": l["product_id"],
                          "product_id_2": r["product_id"],
                          "customer_id": l["customer_id"]}
        )
        .select(lambda x: x["product_id_1"] < x["product_id_2"])
        .group_by_count(
            lambda x: {"product_id_1": x["product_id_1"], "product_id_2": x["product_id_2"]},
            lambda x, y: {"product_id_1": x["product_id_1"], "product_id_2": x["product_id_2"], "cross_purchases": y}
        )
        .to_value()
    )
    root_tn._from_zSet_function = root_tn.zSet_to_debezium
    #
    root_tn.build()
    #
    return root_tn
