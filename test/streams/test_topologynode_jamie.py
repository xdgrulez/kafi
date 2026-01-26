import os, random, sys, time
#
import cloudpickle as pickle

#

if os.path.basename(os.getcwd()) == "kafi":
    sys.path.insert(1, ".")
else:
    sys.path.insert(1, "../..")

from kafi.streams.topologynode import get, update, sum, agg_tuple, message_dict_list_to_ZSet, source

#

def setup():
    transactions_source_topologyNode = source("transactions")
    #
# create view credits as select to_account as account, sum(amount) as credits from transactions group by to_account;
    credits_topologyNode = (
        transactions_source_topologyNode
        .group_by_agg(by_function_list=[get("to_account")],
                      as_function=update("account"),
                      agg_tuple_list=[agg_tuple(select_function=get("amount"),
                                                agg_function=sum,
                                                as_function=update("credits"))])
        # .group_by_agg(by_function_list=[lambda x: x["to_account"]],
        #               as_function=lambda x, y: x.update({"account": y}),
        #               agg_tuple_list=[agg_tuple(select_function=lambda x: x["amount"],
        #                                         agg_function=lambda x, y: x + y,
        #                                         as_function=lambda x, y: x.update({"credits": y}))])
    )
    
# create view debits as select from_account as account, sum(amount) as debits from transactions group by from_account;
    debits_topologyNode = (
        transactions_source_topologyNode
        .group_by_agg(by_function_list=[get("from_account")],
                      as_function=update("account"),
                      agg_tuple_list=[agg_tuple(select_function=get("amount"),
                                                agg_function=sum,
                                                as_function=update("debits"))])
        # .group_by_agg(by_function_list=[lambda x: x["from_account"]],
        #               as_function=lambda x, y: x.update({"account": y}),
        #               agg_tuple_list=[agg_tuple(select_function=lambda x: x["amount"],
        #                                         agg_function=lambda x, y: x + y,
        #                                         as_function=lambda x, y: x.update({"debits": y}))])
    )
    #
# create view balance as select credits.account as account, credits - debits as balance from credits inner join debits on credits.account = debits.account;
    balance_topologyNode = (
        credits_topologyNode
        .join(
            debits_topologyNode,
            on_function=lambda l, r: l["account"] == r["account"],
            projection_function=lambda l, r: {"account": l["account"],
                                              "balance": l["credits"] - r["debits"]}
        )
    )
    #
# create view total as select sum(balance) from balance;
    root_topologyNode = (
        balance_topologyNode
        .agg(agg_tuple_list=[agg_tuple(select_function=get("balance"), 
                                       agg_function=sum,
                                       as_function=update("sum"))])
        # .agg(agg_tuple_list=[agg_tuple(select_function=lambda x: x["balance"], 
        #                                agg_function=lambda x, y: x + y,
        #                                as_function=lambda x, y: x.update({"sum": y}))])
    )
    #
    return transactions_source_topologyNode, root_topologyNode

#

def transactions(transactions_source_topologyNode, root_topologyNode):
    n_int = 10000
    random.seed(42)
    transactions_message_dict_list = []
    for id_int in range(0, n_int):
        message_dict = {"key": str(id_int),
                        "value": {"from_account": random.randint(0, 9),
                                  "to_account": random.randint(0, 9),
                                  "amount": 1}}
        transactions_message_dict_list.append(message_dict)
    #
    transactions_zset = message_dict_list_to_ZSet(transactions_message_dict_list)
    transactions_source_topologyNode.output_handle_function().get().send(transactions_zset)
    #
    root_topologyNode.step()
    root_topologyNode.gc()

#

transactions_source_topologyNode, root_topologyNode = setup()

#

start_time = time.time()
for i in range(1):
    print(i)
    #
    transactions(transactions_source_topologyNode, root_topologyNode)
    #
    print()
    print(f"Latest: {root_topologyNode.latest()}")
    print(len(pickle.dumps(root_topologyNode)) / 1024 / 1024)
#
end_time = time.time()
print(end_time - start_time)

#

print()
print(f"Topology: {root_topologyNode.topology()}")
print()
print(f"Topology: {root_topologyNode.topology(True)}")
print()
print(f"Mermaid:\n{root_topologyNode.mermaid()}")
print()
print(f"Mermaid:\n{root_topologyNode.mermaid(True)}")
print()
print(f"Latest: {root_topologyNode.latest()}")
