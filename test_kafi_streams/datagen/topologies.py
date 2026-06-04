from kafi.streams.topologynode import source

#

def get_root_tn_datagen_1_join(click_source_str, customer_source_str):
    click_source_tn = source(click_source_str)
    customer_source_tn = source(customer_source_str)
    #
    click_tn = (
        click_source_tn
        .map(lambda x: {"user_id": x["value"]["user_id"], "ip": x["value"]["ip"]})
        .distinct()
    )
    #
    customer_tn = (
        customer_source_tn
        .map(lambda x: {"id": x["value"]["id"], "first_name": x["value"]["first_name"]})
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
        .map(lambda x: {"user_id": x["value"]["user_id"], "ip": x["value"]["ip"], "product_id": x["value"]["product_id"]})
        .distinct()
    )
    #
    customer_tn = (
        customer_source_tn
        .map(lambda x: {"id": x["value"]["id"], "first_name": x["value"]["first_name"]})
        .distinct()
    )
    #
    product_tn = (
        product_source_tn
        .map(lambda x: {"id": x["value"]["id"], "brand": x["value"]["brand"]})
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
        .map(lambda x: {"user_id": x["value"]["user_id"], "ip": x["value"]["ip"], "product_id": x["value"]["product_id"]})
        .distinct()
    )
    #
    customer_tn = (
        customer_source_tn
        .map(lambda x: {"id": x["value"]["id"], "first_name": x["value"]["first_name"]})
        .distinct()
    )
    #
    product_tn = (
        product_source_tn
        .map(lambda x: {"id": x["value"]["id"], "brand": x["value"]["brand"]})
        .distinct()
    )
    #
    order_tn = (
        order_source_tn
        .map(lambda x: {"order_id": x["value"]["order_id"], "product_id": x["value"]["product_id"], "customer_id": x["value"]["customer_id"]})
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

def get_root_tn_datagen_self_join_group_by(customer_source_str, product_source_str, order_source_str):
    customer_source_tn = source(customer_source_str)
    product_source_tn = source(product_source_str)
    order_source_tn = source(order_source_str)
    #
    order_tn = (
        order_source_tn
        .map(lambda x: {"order_id": x["value"]["order_id"], "product_id": x["value"]["product_id"], "customer_id": x["value"]["customer_id"]})
        .distinct()
    )
    #
    product_tn = (
        product_source_tn
        .map(lambda x: {"id": x["value"]["id"], "brand": x["value"]["brand"]})
        .distinct()
    )
    #
    customer_tn = (
        customer_source_tn
        .map(lambda x: {"id": x["value"]["id"], "first_name": x["value"]["first_name"]})
        .distinct()
    )
    #
    enriched_order_tn = (
        order_tn
        .join_equi(
            product_tn,
            lambda l: l["product_id"],
            lambda r: r["id"],
            lambda l, r: {"order_id": l["order_id"],
                          "product_id": l["product_id"],
                          "customer_id": l["customer_id"],
                          "brand": r["brand"]})
        .join_equi(
            customer_tn,
            lambda l: l["customer_id"],
            lambda r: r["id"],
            lambda l, r: {"order_id": l["order_id"],
                          "product_id": l["product_id"],
                          "customer_id": l["customer_id"],
                          "brand": l["brand"],
                          "first_name": r["first_name"]})
    )
    #
#     SELECT 
#     p1.product_id AS produkt_A, 
#     p2.product_id AS produkt_B,
#     COUNT(DISTINCT p1.customer_id) AS gemeinsame_kunden
# FROM 
#     deine_tabelle p1
# JOIN 
#     deine_tabelle p2 ON p1.customer_id = p2.customer_id
# WHERE 
#     p1.product_id < p2.product_id
# GROUP BY 
#     p1.product_id, p2.product_id
    root_tn = (
        enriched_order_tn.peek(print)
        .join(
            enriched_order_tn,
            lambda l, r: l["customer_id"] == r["customer_id"] and l["product_id"] < r["product_id"],
            lambda l, r: {"product_id_1": l["product_id"],
                          "product_id_2": r["product_id"],
                          "customer_id": l["customer_id"]}
        ).distinct()
        .group_by_count(
            lambda x: {"product_id_1": x["product_id_1"], "product_id_2": x["product_id_2"]},
            lambda x, y: {"product_id_1": x["product_id_1"], "product_id_2": x["product_id_2"], "count": y}
        )
        .to_value()
    )
    #
    root_tn.build()
    #
    return root_tn
