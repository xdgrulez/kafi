from kafi.streams.topologynode import (
    source
)

from datagen.shoe_clickstream import ShoeClickstreamGenerator
from datagen.shoe_customers import ShoeCustomerGenerator
from datagen.shoes import ShoeProductGenerator 
from datagen.shoe_orders import ShoeOrderGenerator
from jamie.transactions import TransactionGenerator
from wc.plaintext import PlainTextGenerator

#

class TestBase():
    def get_datagen_1_join_root_tn(self, click_source_str, customer_source_str):
        click_source_tn = source(click_source_str)
        customer_source_tn = source(customer_source_str)
        #
        click_tn = (
            click_source_tn
            .map(lambda x: {"user_id": x["user_id"], "ip": x["ip"]})
        )
        #
        customer_tn = (
            customer_source_tn
            .map(lambda x: {"id": x["id"], "first_name": x["first_name"]})
        )
        #
        root_tn = (
            click_tn
            .join_equi(
                customer_tn,
                lambda l: l["user_id"],
                lambda r: r["id"],
                projection_function=lambda l, r: {
                    "user_id": l["user_id"],
                    "ip": l["ip"],
                    "first_name": r["first_name"]})
        )
        # root_tn = (
        #     click_tn
        #     .join(customer_tn,
        #           lambda l, r: l["user_id"] == r["id"],
        #           projection_function=lambda l, r: {
        #               "user_id": l["user_id"],
        #               "ip": l["ip"],
        #               "first_name": r["first_name"]})
        # )
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
            .map(lambda x: {"user_id": x["user_id"], "ip": x["ip"], "product_id": x["product_id"]})
        )
        #
        customer_tn = (
            customer_source_tn
            .map(lambda x: {"id": x["id"], "first_name": x["first_name"]})
        )
        #
        product_tn = (
            product_source_tn
            .map(lambda x: {"id": x["id"], "brand": x["brand"]})
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
                lambda l, r: {"user_id": l["user_id"],
                              "ip": l["ip"],
                              "product_id": l["product_id"],
                              "first_name": l["first_name"],
                              "brand": r["brand"]})
        )
        # root_tn = (
        #     click_tn
        #     .join(customer_tn,
        #           lambda l, r: l["user_id"] == r["id"],
        #           lambda l, r: {
        #               "user_id": l["user_id"],
        #               "ip": l["ip"],
        #               "product_id": l["product_id"],
        #               "first_name": r["first_name"]})
        #               .join(product_tn,
        #                     lambda l, r: l["product_id"] == r["id"],
        #                     lambda l, r: {"user_id": l["user_id"],
        #                                   "ip": l["ip"],
        #                                   "product_id": l["product_id"],
        #                                   "first_name": l["first_name"],
        #                                   "brand": r["brand"]})
        # )
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
            .map(lambda x: {"user_id": x["user_id"], "ip": x["ip"], "product_id": x["product_id"]})
        )
        #
        customer_tn = (
            customer_source_tn
            .map(lambda x: {"id": x["id"], "first_name": x["first_name"]})
        )
        #
        product_tn = (
            product_source_tn
            .map(lambda x: {"id": x["id"], "brand": x["brand"]})
        )
        #
        order_tn = (
            order_source_tn
            .map(lambda x: {"order_id": x["order_id"], "product_id": x["product_id"], "customer_id": x["customer_id"]})
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
                lambda l, r: {
                    "user_id": l["user_id"],
                    "ip": l["ip"],
                    "product_id": l["product_id"],
                    "first_name": l["first_name"],
                    "brand": r["brand"],
                    "order_id": l["order_id"]})
        )
        # root_tn = (
        #     click_tn.join(
        #         order_tn,
        #         lambda l, r: l["product_id"] == r["product_id"] and l["user_id"] == r["customer_id"],
        #         lambda l, r: {
        #             "user_id": l["user_id"],
        #             "ip": l["ip"],
        #             "product_id": l["product_id"],
        #             "order_id": r["order_id"]})
        #             .join(
        #                 customer_tn,
        #                 lambda l, r: l["user_id"] == r["id"],
        #                 lambda l, r: {
        #                     "user_id": l["user_id"],
        #                     "ip": l["ip"],
        #                     "product_id": l["product_id"],
        #                     "order_id": l["order_id"],
        #                     "first_name": r["first_name"]})
        #                     .join(
        #                         product_tn,
        #                         lambda l, r: l["product_id"] == r["id"],
        #                         lambda l, r: {
        #                             "user_id": l["user_id"],
        #                             "ip": l["ip"],
        #                             "product_id": l["product_id"],
        #                             "first_name": l["first_name"],
        #                             "brand": r["brand"],
        #                             "order_id": l["order_id"]})
        # )
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
                "from_account": x["from_account"],
                "to_account": x["to_account"],
                "amount": x["amount"]
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
        # balance_tn = credits_tn.join(
        #     debits_tn,
        #     lambda l, r: l["account"] == r["account"],
        #     lambda l, r: {
        #         "account": l["account"],
        #         "balance": l["credits"] - r["debits"]
        #     }
        # )
        #
        root_tn = balance_tn.sum(
            lambda x: x["balance"],
            lambda _, y: {"sum": y}
        )
        #
        root_tn.build()
        #
        return root_tn
    
    #

    def get_wc_root_tn(self, plain_text_str):
        _source_tn = source(plain_text_str)
        #
        split_tn = _source_tn.flatmap(
              lambda x: [{"word": word_str} for word_str in x["text"].split()]
        )
        #
        root_tn = split_tn.group_by_count(
            lambda x: x["word"],
            lambda x, y: {"word": x,
                          "count": y}
        )
        #
        root_tn.build()
        #
        return root_tn

    ###

    def init_generate(self, source_str):
        match source_str:
            case "shoe_clickstream":
                self.generator_dict[source_str] = ShoeClickstreamGenerator()
            case "shoe_customers":
                self.generator_dict[source_str] = ShoeCustomerGenerator()
            case "shoes":
                self.generator_dict[source_str] = ShoeProductGenerator()
            case "shoe_orders":
                self.generator_dict[source_str] = ShoeOrderGenerator()
            #
            case "transactions": 
                self.generator_dict[source_str] = TransactionGenerator()
            #
            case "plain_text":
                self.generator_dict[source_str] = PlainTextGenerator()
            case _:
                raise Exception(f"Source not supported: {source_str}")

    def generate(self, source_str, batch_size_int):
        message_dict_list = []
        #
        generator = self.generator_dict[source_str]
        #
        for _ in range(batch_size_int):
            record_dict = generator.generate_record()
            message_dict = {"key": None,
                            "value": record_dict}
            message_dict_list.append(message_dict)
        #
        return message_dict_list
