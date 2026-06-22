from kafi.streams.topologynode import TopologyNode as Tn

from test.streams.datagen.shoe_orders import ts_step_int

#

def get_root_tn_datagen_1_join(click_source_str, customer_source_str, join_1_sink_str):
    click_source_tn = Tn.source(click_source_str)
    customer_source_tn = Tn.source(customer_source_str)
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
    join_1_tn = (
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
    root_tn = Tn.sink(join_1_sink_str, join_1_tn)
    #
    root_tn.build()
    #
    return root_tn

def get_root_tn_datagen_2_joins(click_source_str, customer_source_str, product_source_str, joins_2_sink_str):
    click_source_tn = Tn.source(click_source_str)
    customer_source_tn = Tn.source(customer_source_str)
    product_source_tn = Tn.source(product_source_str)
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
    joins_2_tn = (
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
    root_tn = Tn.sink(joins_2_sink_str, joins_2_tn)
    #
    root_tn.build()
    #
    return root_tn

def get_root_tn_datagen_3_joins(click_source_str, customer_source_str, product_source_str, order_source_str, joins_3_sink_str):
    click_source_tn = Tn.source(click_source_str)
    customer_source_tn = Tn.source(customer_source_str)
    product_source_tn = Tn.source(product_source_str)
    order_source_tn = Tn.source(order_source_str)
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
    joins_3_tn = (
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
    root_tn = Tn.sink(joins_3_sink_str, joins_3_tn)
    #
    root_tn.build()
    #
    return root_tn

def get_root_tn_datagen_self_join_group_by(order_source_str, self_join_group_by_sink_str):
    order_source_tn = Tn.source(order_source_str)
    #
    order_tn = (
        order_source_tn
        .from_value()
        .map(lambda x: {"product_id": x["product_id"], "customer_id": x["customer_id"]})
        .distinct()
    )
    #
    self_join_group_by_tn = (
        order_tn
        .join_equi(
            order_tn,
            lambda l: l["customer_id"],
            lambda r: r["customer_id"],
            lambda l, r: {"product_id_1": l["product_id"],
                          "product_id_2": r["product_id"],
                          "customer_id": l["customer_id"]}
        )
        .filter(lambda x: x["product_id_1"] < x["product_id_2"])
        .distinct()
        .group_by_count(
            lambda x: {"product_id_1": x["product_id_1"], "product_id_2": x["product_id_2"]},
            lambda x, y: {"product_id_1": x["product_id_1"], "product_id_2": x["product_id_2"], "cross_purchases": y}
        )
        .to_value()
    )
    #
    root_tn = Tn.sink(self_join_group_by_sink_str, self_join_group_by_tn)
    #
    root_tn.build()
    #
    return root_tn

def get_root_tn_datagen_self_join_group_by_debezium(order_source_str, self_join_group_by_debezium_sink_str):
    order_source_tn = Tn.source(order_source_str)
    order_source_tn._to_zSet_function = order_source_tn.from_debezium
    #
    order_tn = (
        order_source_tn
        .from_value()
        .map(lambda x: {"product_id": x["product_id"], "customer_id": x["customer_id"]})
        .distinct()
    )
    #
    self_join_group_by_debezium_tn = (
        order_tn
        .join_equi(
            order_tn,
            lambda l: l["customer_id"],
            lambda r: r["customer_id"],
            lambda l, r: {"product_id_1": l["product_id"],
                          "product_id_2": r["product_id"],
                          "customer_id": l["customer_id"]}
        )
        .filter(lambda x: x["product_id_1"] < x["product_id_2"])
        .group_by_count(
            lambda x: {"product_id_1": x["product_id_1"], "product_id_2": x["product_id_2"]},
            lambda x, y: {"product_id_1": x["product_id_1"], "product_id_2": x["product_id_2"], "cross_purchases": y}
        )
        .to_value()
    )
    #
    root_tn = Tn.sink(self_join_group_by_debezium_sink_str, self_join_group_by_debezium_tn)
    root_tn._from_zSet_function = root_tn.to_debezium
    #
    root_tn.build()
    #
    return root_tn

def get_built_tn_datagen_multiple_sinks(get_source_tn_function, get_sink_customer_a_h_function, get_sink_customer_i_q_function, get_sink_customer_r_z_function):
    customer_source_tn = get_source_tn_function()
    #
    customer_tn = (
        customer_source_tn
        .from_value()
        .map(lambda x: {"id": x["id"], "last_name": x["last_name"]})
        .distinct()
    )
    #
    customer_a_h_tn = (
        customer_tn
        .filter(lambda x: x["last_name"][0].lower() >= "A".lower() and x["last_name"][0].lower() <= "H".lower()).to_value()
    )
    sink_customer_a_h_tn = get_sink_customer_a_h_function(customer_a_h_tn)
    #
    customer_i_q_tn = (
        customer_tn
        .filter(lambda x: x["last_name"][0].lower() >= "I".lower() and x["last_name"][0].lower() <= "Q".lower()).to_value()
    )
    sink_customer_i_q_tn = get_sink_customer_i_q_function(customer_i_q_tn)
    #
    customer_r_z_tn = (
        customer_tn
        .filter(lambda x: x["last_name"][0].lower() >= "R".lower() and x["last_name"][0].lower() <= "Z".lower()).to_value()
    )
    sink_customer_r_z_tn = get_sink_customer_r_z_function(customer_r_z_tn)
    #
    built_tn = Tn.build(sink_customer_a_h_tn, sink_customer_i_q_tn, sink_customer_r_z_tn)
    #
    return built_tn

def get_built_tn_datagen_expire(get_source_tn_function, get_sink_tn_function):
    order_source_tn = get_source_tn_function()
    #
    expire_tn = order_source_tn.expire(
        lambda x: x["value"]["ts"],
        lambda x: x["value"]["ts"] + ts_step_int * 10,
        lambda x: (x[0]["value"].__setitem__("expiry", x[1]) or x[0])
    )
    #
    sink_tn = get_sink_tn_function(expire_tn)
    #
    built_tn = Tn.build(sink_tn)
    built_tn._from_zSet_function = built_tn.to_debezium
    #
    return built_tn
