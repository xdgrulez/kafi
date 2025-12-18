import datetime, json, random
import json
from functools import reduce

from pydbsp.zset import ZSet

from topologynode import source, message_dict_list_to_ZSet

# create table if not exists transactions (
#    id int, from_account int, to_account int, amount int, ts timestamp
# )
# with (
#    connector='kafka',
#    topic='transactions',
#    properties.bootstrap.server='broker:29092',
#    scan.startup.mode='earliest',
#    scan.startup.timestamp_millis='140000000'
# )
# row format json;
# create view credits as select to_account as account, sum(amount) as credits from transactions group by to_account;
# create view debits as select from_account as account, sum(amount) as debits from transactions group by from_account;
# create view balance as select credits.account as account, credits - debits as balance from credits inner join debits on credits.account = debits.account;
# create materialized view total as select sum(balance) from balance;
# create sink total_sink from total
# with (
#    connector='kafka',
#    properties.bootstrap.server='broker:29092',
#    topic='total_risingwave',
#    type='append-only',
#    force_append_only='true'
# );

# drop sink total_sink;
# drop materialized view total;
# drop view balance;
# drop view debits;
# drop view credits;
# drop table transactions;

def get_value(any, key_str_list):
    return reduce(lambda d, key_str: d.get(key_str, {}) if isinstance(d, dict) else None, key_str_list, any)


def set_value(d, key_str_list, any):
    for key in key_str_list[:-1]:
        if key not in d or not isinstance(d[key], dict):
            d[key] = {}
        d = d[key]
    d[key_str_list[-1]] = any


def by_function(key_str_list):
    def by_function1(message_dict):
        return get_value(message_dict, ["value"] + key_str_list)
    #
    return by_function1

def none_by_function():
    def none_by_function1(_):
        return None
    #
    return none_by_function1

# def zset_sum(input: ZSet[tuple[I, ZSet[int]]]) -> ZSet[tuple[I, int]]:
#     output_dict: dict[I, int] = {}
#     for (group, zset), _ in input.items():
#         for k, v in zset.items():
#             if group not in output_dict:
#                 output_dict[group] = k[1] * v
#             else:
#                 output_dict[group] += (k[1] * v)
    
#     return ZSet({(group_fst, v): 1 for group_fst, v in output_dict.items()})

def select_function(key_str_list):
    def select_function1(message_dict):
        return get_value(message_dict, ["value"] + key_str_list)
    #
    return select_function1

def agg_as_function(key_str_list):
    def agg_as_function1(message_dict):
        return get_value(message_dict, ["value"] + key_str_list)
    #
    def agg_as_function2(message_dict, any):
        set_value(message_dict, ["value"] + key_str_list, any)
        return message_dict
    #
    return agg_as_function1, agg_as_function2


def group_as_function(key_str_list):
    def group_as_function1(message_dict, any):
        set_value(message_dict, ["value"] + key_str_list, any)
        return message_dict
    #
    return group_as_function1


def sum_function(select_function_agg_as_function1_agg_as_function2_group_as_function_tuple_list): 
    def sum_function1(group_any_zset_tuple_zset):
        agg_group_any_message_str_dict = {}
        for select_function, (agg_as_function1, agg_as_function2), group_as_function in select_function_agg_as_function1_agg_as_function2_group_as_function_tuple_list:
            for (group_any, zset), _ in group_any_zset_tuple_zset.items():
                for message_str, weight_int in zset.items():
                    message_dict = json.loads(message_str)
                    if group_any not in agg_group_any_message_str_dict:
                        any = select_function(message_dict)
                        message_dict1 = {}
                        message_dict1 = agg_as_function2(message_dict1, any * weight_int)
                    else:
                        any = select_function(message_dict)
                        message_dict1 = json.loads(agg_group_any_message_str_dict[group_any])
                        any1 = agg_as_function1(message_dict1)
                        message_dict1 = agg_as_function2(message_dict1, any1 + any * weight_int)
                    #
                    if group_any is not None:
                        message_dict1 = group_as_function(message_dict1, group_any)
                    agg_group_any_message_str_dict[group_any] = json.dumps(message_dict1)
        #
        return ZSet({message_str: 1 for _, message_str in agg_group_any_message_str_dict.items()})
    #
    return sum_function1


def on_function(left_message_dict, right_message_dict):
    return left_message_dict["value"]["account"] == right_message_dict["value"]["account"]

def proj_function(left_message_dict, right_message_dict):
    message_dict = {"value": {"account": left_message_dict["value"]["account"],
                              "balance": left_message_dict["value"]["credits"] - right_message_dict["value"]["debits"]}}
    return message_dict

#

def setup():
    transactions_source_topologyNode = source("transactions")
    #
# create view credits as select to_account as account, sum(amount) as credits from transactions group by to_account;
    credits_topologyNode = (
        transactions_source_topologyNode
        .groupBy(by_function(["to_account"]), sum_function([(select_function(["amount"]), agg_as_function(["credits"]), group_as_function(["account"]))]))
    )
    
# create view debits as select from_account as account, sum(amount) as debits from transactions group by from_account;
    debits_topologyNode = (
        transactions_source_topologyNode
        .groupBy(by_function(["from_account"]), sum_function([(select_function(["amount"]), agg_as_function(["debits"]), group_as_function(["account"]))]))
    )
    #
# create view balance as select credits.account as account, credits - debits as balance from credits inner join debits on credits.account = debits.account;
    balance_topologyNode = (
        credits_topologyNode
        .join(
            debits_topologyNode,
            on_function=on_function,
            projection_function=proj_function
        )
    )
    #
# create view total as select sum(balance) from balance;
    root_topologyNode = (
        balance_topologyNode
        .groupBy(none_by_function(), sum_function([(select_function(["balance"]), agg_as_function(["sum"]), None)]))
    )

    return transactions_source_topologyNode, root_topologyNode

transactions_source_topologyNode, root_topologyNode = setup()

#

n_int = 10000
random.seed(42)
transactions_message_dict_list = []
for id_int in range(0, n_int):
  message_dict = {"key": str(id_int),
                  "value": {"id": id_int,
                            "from_account": random.randint(0, 9),
                            "to_account": random.randint(0, 9),
                            "amount": 1,
                            "ts": datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")}}
  transactions_message_dict_list.append(message_dict)

#

transactions_zset = message_dict_list_to_ZSet(transactions_message_dict_list)
transactions_source_topologyNode.output_handle_function()().get().send(transactions_zset)

root_topologyNode.step()

print()
print(f"Topology: {root_topologyNode.topology()}")
print()
print(f"Mermaid:\n{root_topologyNode.mermaid()}")
print()
print(f"Topology: {root_topologyNode.topology(True)}")
print()
print(f"Mermaid:\n{root_topologyNode.mermaid(True)}")
print()
print(f"Latest: {root_topologyNode.latest()}")
