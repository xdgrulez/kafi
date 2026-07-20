from kafi.streams.topologynode import TopologyNode as Tn

from test.streams.datagen.shoe_orders import ts_step_int

from streams.test_base import default_batch_size_int

#

def get_built_tn_datagen_1_join(get_click_source_tn_fun, 
                                get_customer_source_tn_fun,
                                get_sink_tn_fun):
    click_source_tn = get_click_source_tn_fun()
    customer_source_tn = get_customer_source_tn_fun()
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
    sink_tn = get_sink_tn_fun(join_1_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

def get_built_tn_datagen_2_joins(get_click_source_tn_fun,
                                 get_customer_source_tn_fun, 
                                 get_product_source_tn_fun, 
                                 get_sink_tn_fun):
    click_source_tn = get_click_source_tn_fun()
    customer_source_tn = get_customer_source_tn_fun()
    product_source_tn = get_product_source_tn_fun()
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
    sink_tn = get_sink_tn_fun(joins_2_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

def get_built_tn_datagen_3_joins(get_click_source_tn_fun, 
                                 get_customer_source_tn_fun,
                                 get_product_source_tn_fun,
                                 get_order_source_tn_fun,
                                 get_sink_tn_fun):
    click_source_tn = get_click_source_tn_fun()
    customer_source_tn = get_customer_source_tn_fun()
    product_source_tn = get_product_source_tn_fun()
    order_source_tn = get_order_source_tn_fun()
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
    sink_tn = get_sink_tn_fun(joins_3_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

def get_built_tn_datagen_self_join_group_by(get_source_tn_fun, get_sink_tn_fun):
    order_source_tn = get_source_tn_fun()
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
    sink_tn = get_sink_tn_fun(self_join_group_by_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

def get_built_tn_datagen_self_join_group_by_debezium(get_source_tn_fun, get_sink_tn_fun):
    order_source_tn = get_source_tn_fun()
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
    sink_tn = get_sink_tn_fun(self_join_group_by_debezium_tn)
    #
    built_tn = Tn.build(sink_tn)
    built_tn.from_zSet(Tn.to_debezium)
    #
    return built_tn

def get_built_tn_datagen_multiple_sinks(get_source_tn_fun, get_sink_customer_a_h_fun, get_sink_customer_i_q_fun, get_sink_customer_r_z_fun):
    customer_source_tn = get_source_tn_fun()
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
    sink_customer_a_h_tn = get_sink_customer_a_h_fun(customer_a_h_tn)
    #
    customer_i_q_tn = (
        customer_tn
        .filter(lambda x: x["last_name"][0].lower() >= "I".lower() and x["last_name"][0].lower() <= "Q".lower()).to_value()
    )
    sink_customer_i_q_tn = get_sink_customer_i_q_fun(customer_i_q_tn)
    #
    customer_r_z_tn = (
        customer_tn
        .filter(lambda x: x["last_name"][0].lower() >= "R".lower() and x["last_name"][0].lower() <= "Z".lower()).to_value()
    )
    sink_customer_r_z_tn = get_sink_customer_r_z_fun(customer_r_z_tn)
    #
    built_tn = Tn.build(sink_customer_a_h_tn, sink_customer_i_q_tn, sink_customer_r_z_tn)
    #
    return built_tn

#

def by_fun(x):
    return (x["customer_id"], x["email"])

def agg_fun(agg, x):
    return {
        "orders": agg["orders"] + 1,
        "total_price": agg["total_price"] + x["sale_price"]
    }

agg_initial_any = {"orders": 0, "total_price": 0}

def projection_fun(by, agg):
    return {
        "customer_id": by[0],
        "email": by[1],
        "orders": agg["orders"],
        "total_price": agg["total_price"]
    }

#

def _get_built_tn_datagen_window(get_order_source_tn_fun,
                                 get_customer_source_tn_fun,
                                 get_product_source_tn_fun,
                                 get_sink_tn_fun,
                                 window_dict):
    order_source_tn = get_order_source_tn_fun()
    customer_source_tn = get_customer_source_tn_fun()
    product_source_tn = get_product_source_tn_fun()
    #
    order_tn = (
        order_source_tn
        .map(lambda x: {"product_id": x["value"]["product_id"],
                        "customer_id": x["value"]["customer_id"],
                        "ts": x["value"]["ts"]})
    )
    #
    time_fun = lambda x: x["ts"]
    match window_dict["type"]:
        case "tumbling":
            retention = lambda tn: tn.expire_tumbling(time_fun,
                                                      window_dict["size"],
                                                      window_dict["allowed_lateness"])
        case "hopping":
            retention = lambda tn: tn.expire_hopping(time_fun,
                                                     window_dict["size"],
                                                     window_dict["hop"],
                                                     window_dict["allowed_lateness"])
        case "cumulative":
            retention = lambda tn: tn.expire_cumulative(time_fun,
                                                        window_dict["size"],
                                                        window_dict["advance"],
                                                        window_dict["allowed_lateness"])
        case "sliding":
            retention = lambda tn: tn.expire_sliding(time_fun,
                                                     window_dict["size"],
                                                     window_dict["allowed_lateness"])
        case "session":
            retention = lambda tn: tn.expire_session(time_fun,
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
            window_tn = join_2_tn.window_tumbling(window_dict["size"],
                                                  time_fun,
                                                  by_fun,
                                                  agg_fun,
                                                  agg_initial_any,
                                                  projection_fun,
                                                  trigger_projection_fun=lambda l: {**l[0], "window_end": l[1]})
        case "hopping":
            window_tn = join_2_tn.window_hopping(window_dict["size"],
                                                 window_dict["hop"],
                                                 time_fun,
                                                 by_fun,
                                                 agg_fun,
                                                 agg_initial_any,
                                                 projection_fun,
                                                 trigger_projection_fun=lambda l: {**l[0], "window_end": l[1]})
        case "cumulative":
            window_tn = join_2_tn.window_cumulative(window_dict["size"],
                                                    window_dict["advance"],
                                                    time_fun,
                                                    by_fun,
                                                    agg_fun,
                                                    agg_initial_any,
                                                    projection_fun,
                                                    trigger_projection_fun=lambda l: {**l[0], "window_end": l[1]})
        case "sliding":
            window_tn = join_2_tn.window_sliding(window_dict["size"],
                                                 time_fun,
                                                 by_fun,
                                                 agg_fun,
                                                 agg_initial_any,
                                                 projection_fun,
                                                 trigger_projection_fun=lambda l: {**l[0], "window_end": l[1]})
        case "session":
            window_tn = join_2_tn.window_session(window_dict["gap"],
                                                 time_fun,
                                                 by_fun,
                                                 agg_fun,
                                                 agg_initial_any,
                                                 projection_fun,
                                                 trigger_projection_fun=lambda l: {**l[0], "window_end": l[1]})
    #
    window_tn = window_tn.to_value()
    #
    sink_tn = get_sink_tn_fun(window_tn)
    #
    built_tn = Tn.build(sink_tn)
    #
    return built_tn

def get_built_tn_datagen_tumbling_window(get_order_source_tn_fun,
                                         get_customer_source_tn_fun,
                                         get_product_source_tn_fun,
                                         get_sink_tn_fun):
    built_tn = _get_built_tn_datagen_window(get_order_source_tn_fun,
                                            get_customer_source_tn_fun,
                                            get_product_source_tn_fun,
                                            get_sink_tn_fun,
                                            {"type": "tumbling",
                                             "size": (size_int := ts_step_int * default_batch_size_int),
                                             "allowed_lateness": size_int * 5}
                                            )
    #
    return built_tn

def get_built_tn_datagen_hopping_window(get_order_source_tn_fun,
                                        get_customer_source_tn_fun,
                                        get_product_source_tn_fun,
                                        get_sink_tn_fun):
    built_tn = _get_built_tn_datagen_window(get_order_source_tn_fun,
                                            get_customer_source_tn_fun,
                                            get_product_source_tn_fun,
                                            get_sink_tn_fun,
                                            {"type": "hopping",
                                             "size": (size_int := ts_step_int * default_batch_size_int),
                                             "hop": size_int // 5,
                                             "allowed_lateness": size_int * 5}
                                            )
    #
    return built_tn

def get_built_tn_datagen_cumulative_window(get_order_source_tn_fun,
                                           get_customer_source_tn_fun,
                                           get_product_source_tn_fun,
                                           get_sink_tn_fun):
    built_tn = _get_built_tn_datagen_window(get_order_source_tn_fun,
                                            get_customer_source_tn_fun,
                                            get_product_source_tn_fun,
                                            get_sink_tn_fun,
                                            {"type": "cumulative",
                                             "size": (size_int := ts_step_int * default_batch_size_int),
                                             "advance": size_int // 5,
                                             "allowed_lateness": size_int * 5}
                                            )
    #
    return built_tn

def get_built_tn_datagen_sliding_window(get_order_source_tn_fun,
                                        get_customer_source_tn_fun,
                                        get_product_source_tn_fun,
                                        get_sink_tn_fun):
    built_tn = _get_built_tn_datagen_window(get_order_source_tn_fun,
                                            get_customer_source_tn_fun,
                                            get_product_source_tn_fun,
                                            get_sink_tn_fun,
                                            {"type": "sliding",
                                             "size": (size_int := ts_step_int * default_batch_size_int),
                                             "allowed_lateness": size_int * 5}
                                            )
    #
    return built_tn

def get_built_tn_datagen_session_window(get_order_source_tn_fun,
                                        get_customer_source_tn_fun,
                                        get_product_source_tn_fun,
                                        get_sink_tn_fun):
    built_tn = _get_built_tn_datagen_window(get_order_source_tn_fun,
                                            get_customer_source_tn_fun,
                                            get_product_source_tn_fun,
                                            get_sink_tn_fun,
                                            {"type": "session",
                                             "gap": (size_int := ts_step_int * default_batch_size_int),
                                             "max_session": (size_int := ts_step_int * default_batch_size_int * 2),
                                             "allowed_lateness": size_int * 5}
                                            )
    #
    return built_tn
