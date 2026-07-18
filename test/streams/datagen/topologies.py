from kafi.streams.topologynode import TopologyNode as Tn

from test.streams.datagen.shoe_orders import ts_step_int

from streams.test_base import default_batch_size_int

#

def get_built_tn_datagen_1_join(get_click_source_tn_function, 
                                get_customer_source_tn_function,
                                get_sink_tn_function):
    click_source_tn = get_click_source_tn_function()
    customer_source_tn = get_customer_source_tn_function()
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
    sink_tn = get_sink_tn_function(join_1_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

def get_built_tn_datagen_2_joins(get_click_source_tn_function,
                                 get_customer_source_tn_function, 
                                 get_product_source_tn_function, 
                                 get_sink_tn_function):
    click_source_tn = get_click_source_tn_function()
    customer_source_tn = get_customer_source_tn_function()
    product_source_tn = get_product_source_tn_function()
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
    sink_tn = get_sink_tn_function(joins_2_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

def get_built_tn_datagen_3_joins(get_click_source_tn_function, 
                                 get_customer_source_tn_function,
                                 get_product_source_tn_function,
                                 get_order_source_tn_function,
                                 get_sink_tn_function):
    click_source_tn = get_click_source_tn_function()
    customer_source_tn = get_customer_source_tn_function()
    product_source_tn = get_product_source_tn_function()
    order_source_tn = get_order_source_tn_function()
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
    sink_tn = get_sink_tn_function(joins_3_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

def get_built_tn_datagen_self_join_group_by(get_source_tn_function, get_sink_tn_function):
    order_source_tn = get_source_tn_function()
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
    sink_tn = get_sink_tn_function(self_join_group_by_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

def get_built_tn_datagen_self_join_group_by_debezium(get_source_tn_function, get_sink_tn_function):
    order_source_tn = get_source_tn_function()
    order_source_tn.to_zSet(Tn.from_debezium)
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
    sink_tn = get_sink_tn_function(self_join_group_by_debezium_tn)
    #
    built_tn = Tn.build(sink_tn)
    built_tn.from_zSet(Tn.to_debezium)
    #
    return built_tn

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

#

def by_function(x):
    return (x["customer_id"], x["email"])

def agg_function(agg, x, w):
    return {
        "orders": agg["orders"] + w,
        "total_price": agg["total_price"] + x["sale_price"] * w
    }

agg_initial_any = {"orders": 0, "total_price": 0}

def projection_function(by, agg):
    return {
        "customer_id": by[0],
        "email": by[1],
        "orders": agg["orders"],
        "total_price": agg["total_price"]
    }

#

def _get_built_tn_datagen_window(get_order_source_tn_function,
                                 get_customer_source_tn_function,
                                 get_product_source_tn_function,
                                 get_sink_tn_function,
                                 window_dict):
    order_source_tn = get_order_source_tn_function()
    customer_source_tn = get_customer_source_tn_function()
    product_source_tn = get_product_source_tn_function()
    #
    order_tn = (
        order_source_tn
        .map(lambda x: {"product_id": x["value"]["product_id"],
                        "customer_id": x["value"]["customer_id"],
                        "ts": x["value"]["ts"]})
    )
    #
    time_function = lambda x: x["ts"]
    match window_dict["type"]:
        case "tumbling":
            retention = lambda tn: tn.tumbling_retention(time_function,
                                                          window_dict["size"],
                                                          window_dict["allowed_lateness"])
        case "hopping":
            retention = lambda tn: tn.hopping_retention(time_function,
                                                         window_dict["size"],
                                                         window_dict["hop"],
                                                         window_dict["allowed_lateness"])
        case "cumulative":
            retention = lambda tn: tn.cumulative_retention(time_function,
                                                            window_dict["size"],
                                                            window_dict["advance"],
                                                            window_dict["allowed_lateness"])
        case "sliding":
            retention = lambda tn: tn.sliding_retention(time_function,
                                                        window_dict["size"],
                                                        window_dict["allowed_lateness"])
        case "session":
            retention = lambda tn: tn.session_retention(time_function,
                                                        window_dict["max_session"],
                                                        window_dict["allowed_lateness"])
    #
    order_tn = retention(order_tn).distinct()
    #
    customer_tn = (
        customer_source_tn
        .map(lambda x: {"id": x["value"]["id"],
                        "email": x["value"]["email"]})
        .distinct()
    )
    #
    product_tn = (
        product_source_tn
        .map(lambda x: {"id": x["value"]["id"],
                        "sale_price": x["value"]["sale_price"]})
        .distinct()
    )
    #
    join_1_tn = (
        order_tn
        .join_equi(
            customer_tn,
            lambda l: l["customer_id"],
            lambda r: r["id"],
            lambda l, r: {
                "product_id": l["product_id"],
                "customer_id": l["customer_id"],
                "ts": l["ts"],
                "email": r["email"]
            })
    )
    #
    join_2_tn = (
        join_1_tn
        .join_equi(
            product_tn,
            lambda l: l["product_id"],
            lambda r: r["id"],
            lambda l, r: {
                "product_id": l["product_id"],
                "customer_id": l["customer_id"],
                "ts": l["ts"],
                "email": l["email"],
                "sale_price": r["sale_price"]
            }
        )
    )
    #
    match window_dict["type"]:
        case "tumbling":
            window_tn = join_2_tn.tumbling(window_dict["size"],
                                           time_function,
                                           by_function,
                                           agg_function,
                                           agg_initial_any,
                                           projection_function,
                                           trigger_function=lambda l, r: r >= l[1],
                                           trigger_projection_function=lambda l, _: {**l[0], "window_end": l[1]},)
        case "hopping":
            window_tn = join_2_tn.hopping(window_dict["size"],
                                          window_dict["hop"],
                                          time_function,
                                          by_function,
                                          agg_function,
                                          agg_initial_any,
                                          projection_function)
        case "cumulative":
            window_tn = join_2_tn.cumulative(window_dict["size"],
                                             window_dict["advance"],
                                             time_function,
                                             by_function,
                                             agg_function,
                                             agg_initial_any,
                                             projection_function)
        case "sliding":
            window_tn = join_2_tn.sliding(window_dict["size"],
                                          time_function,
                                          by_function,
                                          agg_function,
                                          agg_initial_any,
                                          projection_function)
        case "session":
            window_tn = join_2_tn.session(window_dict["gap"],
                                          time_function,
                                          by_function,
                                          agg_function,
                                          agg_initial_any,
                                          projection_function)
    #
    window_tn = window_tn.to_value()
    #
    sink_tn = get_sink_tn_function(window_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

def get_built_tn_datagen_tumbling_window(get_order_source_tn_function,
                                         get_customer_source_tn_function,
                                         get_product_source_tn_function,
                                         get_sink_tn_function):
    built_tn = _get_built_tn_datagen_window(get_order_source_tn_function,
                                            get_customer_source_tn_function,
                                            get_product_source_tn_function,
                                            get_sink_tn_function,
                                            {"type": "tumbling",
                                             "size": (size_int := ts_step_int * default_batch_size_int),
                                             "allowed_lateness": size_int * 5}
                                            )
    #
    return built_tn

def get_built_tn_datagen_hopping_window(get_order_source_tn_function,
                                        get_customer_source_tn_function,
                                        get_product_source_tn_function,
                                        get_sink_tn_function):
    built_tn = _get_built_tn_datagen_window(get_order_source_tn_function,
                                            get_customer_source_tn_function,
                                            get_product_source_tn_function,
                                            get_sink_tn_function,
                                            {"type": "hopping",
                                             "size": (size_int := ts_step_int * 10),
                                             "hop": size_int // 5,
                                             "allowed_lateness": size_int * 5}
                                            )
    #
    return built_tn

def get_built_tn_datagen_cumulative_window(get_order_source_tn_function,
                                           get_customer_source_tn_function,
                                           get_product_source_tn_function,
                                           get_sink_tn_function):
    built_tn = _get_built_tn_datagen_window(get_order_source_tn_function,
                                            get_customer_source_tn_function,
                                            get_product_source_tn_function,
                                            get_sink_tn_function,
                                            {"type": "cumulative",
                                             "size": (size_int := ts_step_int * 20),
                                             "advance": size_int // 5,
                                             "allowed_lateness": size_int * 5}
                                            )
    #
    return built_tn

def get_built_tn_datagen_sliding_window(get_order_source_tn_function,
                                        get_customer_source_tn_function,
                                        get_product_source_tn_function,
                                        get_sink_tn_function):
    built_tn = _get_built_tn_datagen_window(get_order_source_tn_function,
                                            get_customer_source_tn_function,
                                            get_product_source_tn_function,
                                            get_sink_tn_function,
                                            {"type": "sliding",
                                             "size": (size_int := ts_step_int * 10),
                                             "allowed_lateness": size_int * 5}
                                            )
    #
    return built_tn

def get_built_tn_datagen_session_window(get_order_source_tn_function,
                                        get_customer_source_tn_function,
                                        get_product_source_tn_function,
                                        get_sink_tn_function):
    built_tn = _get_built_tn_datagen_window(get_order_source_tn_function,
                                            get_customer_source_tn_function,
                                            get_product_source_tn_function,
                                            get_sink_tn_function,
                                            {"type": "session",
                                             "gap": (size_int := ts_step_int * 10),
                                             "max_session": size_int * 100,
                                             "allowed_lateness": size_int * 5}
                                            )
    #
    return built_tn
