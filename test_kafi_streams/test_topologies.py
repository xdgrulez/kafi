from kafi.streams.topologynode import source

#

class TestTopologies:
    def get_datagen_1_join_root_tn(self, click_source_str, customer_source_str):
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

    def get_datagen_2_joins_root_tn(self, click_source_str, customer_source_str, product_source_str):
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

    def get_datagen_3_joins_root_tn(self, click_source_str, customer_source_str, product_source_str, order_source_str):
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

    #

    def get_jamie_root_tn(self, transaction_source_str):
        transaction_source_tn = source(transaction_source_str)
        #
        transaction_tn = transaction_source_tn.map(
            lambda x: {
                "from_account": x["value"]["from_account"],
                "to_account": x["value"]["to_account"],
                "amount": x["value"]["amount"]
            }
        )
        #
        credits_tn = transaction_tn.group_by_sum(
            lambda x: x["to_account"],
            lambda x: x["amount"],
            lambda x, y: {"account": x,
                          "credits": y}
        )
        #
        debits_tn = transaction_tn.group_by_sum(
            lambda x: x["from_account"],
            lambda x: x["amount"],
            lambda x, y: {"account": x,
                          "debits": y}
        )
        #
        balance_tn = credits_tn.join_equi(
            debits_tn,
            lambda l: l["account"],
            lambda r: r["account"],
            lambda l, r: {
                "account": l["account"],
                "balance": l["credits"] - r["debits"]
            }
        )
        #
        root_tn = balance_tn.sum(
            lambda x: x["balance"],
            lambda _, y: {"value": 
                          {"sum": y}}
        )
        #
        root_tn.build()
        #
        return root_tn
    
    #

    def get_wc_root_tn(self, lines_str):
        _source_tn = source(lines_str)
        #
        split_tn = _source_tn.flatmap(
              lambda x: [{"word": word_str} for word_str in x["value"].split()]
        )
        #
        root_tn = split_tn.group_by_count(
            lambda x: x["word"],
            lambda x, y: {"value": 
                          {"word": x,
                           "count": y}}
        )
        #
        root_tn.build()
        #
        return root_tn
