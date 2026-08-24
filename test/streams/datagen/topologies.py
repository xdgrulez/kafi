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
        .map(lambda r: {"user_id": r["value"]["user_id"], "ip": r["value"]["ip"]})
        .distinct()
    )
    #
    customer_tn = (
        customer_source_tn
        .map(lambda r: {"id": r["value"]["id"], "first_name": r["value"]["first_name"]})
        .distinct()
    )
    #
    join_1_tn = (
        click_tn
        .join(
            customer_tn,
            lambda l_r: l_r["user_id"],
            lambda r_r: r_r["id"],
            lambda l_r, r_r: {"value": {"user_id": l_r["user_id"],
                                        "ip": l_r["ip"],
                                        "first_name": r_r["first_name"]}})
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
        .map(lambda r: {"user_id": r["value"]["user_id"], "ip": r["value"]["ip"], "product_id": r["value"]["product_id"]})
        .distinct()
    )
    #
    customer_tn = (
        customer_source_tn
        .map(lambda r: {"id": r["value"]["id"], "first_name": r["value"]["first_name"]})
        .distinct()
    )
    #
    product_tn = (
        product_source_tn
        .map(lambda r: {"id": r["value"]["id"], "brand": r["value"]["brand"]})
        .distinct()
    )
    #
    joins_2_tn = (
        click_tn
        .join(
            customer_tn,
            lambda l_r: l_r["user_id"],
            lambda r_r: r_r["id"],
            lambda l_r, r_r: {"user_id": l_r["user_id"],
                              "ip": l_r["ip"],
                              "product_id": l_r["product_id"],
                              "first_name": r_r["first_name"]})
        .join(
            product_tn,
            lambda l_r: l_r["product_id"],
            lambda r_r: r_r["id"],
            lambda l_r, r_r: {"value": {"user_id": l_r["user_id"],
                                        "ip": l_r["ip"],
                                        "product_id": l_r["product_id"],
                                        "first_name": l_r["first_name"],
                                        "brand": r_r["brand"]}})
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
        .map(lambda r: {"user_id": r["value"]["user_id"], "ip": r["value"]["ip"], "product_id": r["value"]["product_id"]})
        .distinct()
    )
    #
    customer_tn = (
        customer_source_tn
        .map(lambda r: {"id": r["value"]["id"], "first_name": r["value"]["first_name"]})
        .distinct()
    )
    #
    product_tn = (
        product_source_tn
        .map(lambda r: {"id": r["value"]["id"], "brand": r["value"]["brand"]})
        .distinct()
    )
    #
    order_tn = (
        order_source_tn
        .map(lambda r: {"order_id": r["value"]["order_id"], "product_id": r["value"]["product_id"], "customer_id": r["value"]["customer_id"]})
        .distinct()
    )
    #
    joins_3_tn = (
        click_tn
        .join(
            order_tn,
            lambda l_r: (l_r["product_id"], l_r["user_id"]),
            lambda r_r: (r_r["product_id"], r_r["customer_id"]),
            lambda l_r, r_r: {"user_id": l_r["user_id"],
                              "ip": l_r["ip"],
                              "product_id": l_r["product_id"],
                              "order_id": r_r["order_id"]})
        .join(
            customer_tn,
            lambda l_r: l_r["user_id"],
            lambda r_r: r_r["id"],
            lambda l_r, r_r: {"user_id": l_r["user_id"],
                              "ip": l_r["ip"],
                              "product_id": l_r["product_id"],
                              "order_id": l_r["order_id"],
                              "first_name": r_r["first_name"]})
        .join(
            product_tn,
            lambda l_r: l_r["product_id"],
            lambda r_r: r_r["id"],
            lambda l_r, r_r: {"value": {"user_id": l_r["user_id"],
                                        "ip": l_r["ip"],
                                        "product_id": l_r["product_id"],
                                        "first_name": l_r["first_name"],
                                        "brand": r_r["brand"],
                                        "order_id": l_r["order_id"]}})
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
        .map(lambda r: r["value"])
        .map(lambda r: {"product_id": r["product_id"], "customer_id": r["customer_id"]})
        .distinct()
    )
    #
    self_join_group_by_tn = (
        order_tn
        .join(
            order_tn,
            lambda l_r: l_r["customer_id"],
            lambda r_r: r_r["customer_id"],
            lambda l_r, r_r: {"product_id_1": l_r["product_id"],
                              "product_id_2": r_r["product_id"],
                              "customer_id": l_r["customer_id"]}
        )
        .filter(lambda r: r["product_id_1"] < r["product_id_2"])
        .distinct()
        .group_by_count(
            lambda r: {"product_id_1": r["product_id_1"], "product_id_2": r["product_id_2"]},
            lambda key_r, agg_r: {"product_id_1": key_r["product_id_1"], "product_id_2": key_r["product_id_2"], "cross_purchases": agg_r}
        )
        .map(lambda r: {"value": r})
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
        .map(lambda r: r["value"])
        .map(lambda r: {"product_id": r["product_id"], "customer_id": r["customer_id"]})
        .distinct()
    )
    #
    self_join_group_by_debezium_tn = (
        order_tn
        .join(
            order_tn,
            lambda l_r: l_r["customer_id"],
            lambda r_r: r_r["customer_id"],
            lambda l_r, r_r: {"product_id_1": l_r["product_id"],
                              "product_id_2": r_r["product_id"],
                              "customer_id": l_r["customer_id"]}
        )
        .filter(lambda r: r["product_id_1"] < r["product_id_2"])
        .group_by_count(
            lambda r: {"product_id_1": r["product_id_1"], "product_id_2": r["product_id_2"]},
            lambda key_r, agg_r: {"product_id_1": key_r["product_id_1"], "product_id_2": key_r["product_id_2"], "cross_purchases": agg_r}
        )
        .map(lambda r: {"value": r})
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
        .map(lambda r: r["value"])
        .map(lambda r: {"id": r["id"], "last_name": r["last_name"]})
        .distinct()
    )
    #
    customer_a_h_tn = (
        customer_tn
        .filter(lambda r: r["last_name"][0].lower() >= "A".lower() and r["last_name"][0].lower() <= "H".lower()).map(lambda r: {"value": r})
    )
    sink_customer_a_h_tn = get_sink_customer_a_h_fun(customer_a_h_tn)
    #
    customer_i_q_tn = (
        customer_tn
        .filter(lambda r: r["last_name"][0].lower() >= "I".lower() and r["last_name"][0].lower() <= "Q".lower()).map(lambda r: {"value": r})
    )
    sink_customer_i_q_tn = get_sink_customer_i_q_fun(customer_i_q_tn)
    #
    customer_r_z_tn = (
        customer_tn
        .filter(lambda r: r["last_name"][0].lower() >= "R".lower() and r["last_name"][0].lower() <= "Z".lower()).map(lambda r: {"value": r})
    )
    sink_customer_r_z_tn = get_sink_customer_r_z_fun(customer_r_z_tn)
    #
    built_tn = Tn.build(sink_customer_a_h_tn, sink_customer_i_q_tn, sink_customer_r_z_tn)
    #
    return built_tn

#

def key_fun(r):
    return (r["customer_id"], r["email"])

def agg_fun(agg_r, r):
    return {
        "orders": agg_r["orders"] + 1,
        "total_price": agg_r["total_price"] + r["sale_price"]
    }

agg_initial_any = {"orders": 0, "total_price": 0}

def project_fun(key_any, agg_r):
    return {
        "customer_id": key_any[0],
        "email": key_any[1],
        "orders": agg_r["orders"],
        "total_price": agg_r["total_price"]
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
        .map(lambda r: {"product_id": r["value"]["product_id"],
                        "customer_id": r["value"]["customer_id"],
                        "ts": r["value"]["ts"]})
    )
    #
    ts_fun = lambda r: r["ts"]
    match window_dict["type"]:
        case "tumbling":
            retention = lambda tn: tn.expire_tumbling(ts_fun,
                                                      window_dict["size"],
                                                      window_dict["allowed_lateness"])
        case "hopping":
            retention = lambda tn: tn.expire_hopping(ts_fun,
                                                     window_dict["size"],
                                                     window_dict["hop"],
                                                     window_dict["allowed_lateness"])
        case "cumulative":
            retention = lambda tn: tn.expire_cumulative(ts_fun,
                                                        window_dict["size"],
                                                        window_dict["step"],
                                                        window_dict["allowed_lateness"])
        case "sliding":
            retention = lambda tn: tn.expire_sliding(ts_fun,
                                                     window_dict["size"],
                                                     window_dict["allowed_lateness"])
        case "session":
            retention = lambda tn: tn.expire_session(ts_fun,
                                                     window_dict["max_session"],
                                                     window_dict["allowed_lateness"])
    #
    order_tn = retention(order_tn).distinct()
    #
    customer_tn = (
        customer_source_tn
        .map(lambda r: {"id": r["value"]["id"],
                        "email": r["value"]["email"]})
        .distinct()
    )
    #
    product_tn = (
        product_source_tn
        .map(lambda r: {"id": r["value"]["id"],
                        "sale_price": r["value"]["sale_price"]})
        .distinct()
    )
    #
    join_1_tn = (
        order_tn
        .join(
            customer_tn,
            lambda l_r: l_r["customer_id"],
            lambda r_r: r_r["id"],
            lambda l_r, r_r: {
                "product_id": l_r["product_id"],
                "customer_id": l_r["customer_id"],
                "ts": l_r["ts"],
                "email": r_r["email"]
            })
    )
    #
    join_2_tn = (
        join_1_tn
        .join(
            product_tn,
            lambda l_r: l_r["product_id"],
            lambda r_r: r_r["id"],
            lambda l_r, r_r: {
                "product_id": l_r["product_id"],
                "customer_id": l_r["customer_id"],
                "ts": l_r["ts"],
                "email": l_r["email"],
                "sale_price": r_r["sale_price"]
            }
        )
    )
    #
    match window_dict["type"]:
        case "tumbling":
            window_tn = join_2_tn.group_by_agg_tumbling(
                ts_fun,
                window_dict["size"],
                key_fun,
                agg_fun,
                agg_initial_any,
                project_fun,
                trigger_projection_fun=lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]})
        case "hopping":
            window_tn = join_2_tn.group_by_agg_hopping(
                ts_fun,
                window_dict["size"],
                window_dict["hop"],
                key_fun,
                agg_fun,
                agg_initial_any,
                project_fun,
                trigger_projection_fun=lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]})
        case "cumulative":
            window_tn = join_2_tn.group_by_agg_cumulative(
                ts_fun,
                window_dict["size"],
                window_dict["step"],
                key_fun,
                agg_fun,
                agg_initial_any,
                project_fun,
                trigger_projection_fun=lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]})
        case "sliding":
            window_tn = join_2_tn.group_by_agg_sliding(
                ts_fun,
                window_dict["size"],
                key_fun,
                agg_fun,
                agg_initial_any,
                project_fun,
                trigger_projection_fun=lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]})
        case "session":
            window_tn = join_2_tn.group_by_agg_session(
                ts_fun,
                window_dict["gap"],
                key_fun,
                agg_fun,
                agg_initial_any,
                project_fun,
                trigger_projection_fun=lambda r_end_ts_tuple: {**r_end_ts_tuple[0], "window_end": r_end_ts_tuple[1]})
    #
    window_tn = window_tn.map(lambda r: {"value": r})
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
                                             "step": size_int // 5,
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
