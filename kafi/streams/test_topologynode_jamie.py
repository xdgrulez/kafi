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


def by_function(message_dict):
    return message_dict["value"]["to_account"]

# def zset_sum(input: ZSet[tuple[I, ZSet[int]]]) -> ZSet[tuple[I, int]]:
#     output_dict: dict[I, int] = {}
#     for (group, zset), _ in input.items():
#         for k, v in zset.items():
#             if group not in output_dict:
#                 output_dict[group] = k[1] * v
#             else:
#                 output_dict[group] += (k[1] * v)
    
#     return ZSet({(group_fst, v): 1 for group_fst, v in output_dict.items()})


def agg_function(group_any_zset_tuple_zset):
    return agg_function1(group_any_zset_tuple_zset, )


def agg_function(group_any_zset_tuple_zset, select_str_key_str_list_as_key_str_list_tuple_dict):
    def agg_function1(group_any_zset_tuple_zset):
        agg_group_any_message_str_dict = {}
        for select_str, (key_str_list, as_key_str_list) in select_str_key_str_list_as_key_str_list_tuple_dict.items():
            if select_str == "sum":
                for group_any, zset in group_any_zset_tuple_zset:
                    for message_str, weight_int in zset.items():
                        message_dict = json.loads(message_str)
                        if group_any not in agg_group_any_message_str_dict:
                            any = get_value(message_dict, ["value"] + key_str_list)
                            message_dict1 = {}
                            set_value(message_dict1, ["value"] + as_key_str_list, any * weight_int)
                            agg_group_any_message_str_dict[group_any] = json.dumps(message_dict1)
                        else:
                            any = get_value(message_dict, key_str_list)
                            message_dict1 = json.loads(agg_group_any_message_str_dict[group_any])
                            any1 = get_value(message_dict1, ["value"] + key_str_list)
                            set_value(message_dict1, ["value"] + as_key_str_list, any1 + any * weight_int)
                            agg_group_any_message_str_dict[group_any] = json.dumps(message_dict1)
        #
        return ZSet({(group_any, message_dict): 1 for group_any, message_dict in agg_group_any_message_str_dict.items()})
    #
    return lambda group_any_zset_tuple_zset: agg_function1(group_any_zset_tuple_zset)


def setup():
    transactions_source_topologyNode = source("transactions")
    #
    root_topologyNode = (
        transactions_source_topologyNode
        .groupBy(by_function, agg_function())
    )
    #
    return transactions_source_topologyNode, salaries_source_topologyNode, root_topologyNode

employees_source_topologyNode, salaries_source_topologyNode, root_topologyNode = setup()

employee_message_dict_list = [{"key": "0", "value": {"name": "kristjan"}},
                            {"key": "1", "value": {"name": "mark"}},
                            {"key": "2", "value": {"name": "mike"}}]
salary_message_dict_list = [{"key": "2", "value": {"salary": 40000}},
                            {"key": "0", "value": {"salary": 38750}},
                            {"key": "1", "value": {"salary": 50000}}]

employee_zset = message_dict_list_to_ZSet(employee_message_dict_list)
salary_zset = message_dict_list_to_ZSet(salary_message_dict_list)
employees_source_topologyNode.output_handle_function()().get().send(employee_zset)
salaries_source_topologyNode.output_handle_function()().get().send(salary_zset)

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

#

root_topologyNode = pickle.loads(pickle.dumps(root_topologyNode))

salary_message_dict_list1 = [{"key": "0", "value": {"salary": 100000}}]
salary_zset1 = message_dict_list_to_ZSet(salary_message_dict_list1)

# salaries_source_topologyNode = root_topologyNode.get_node_by_id(salaries_source_topologyNode.id())
salaries_source_topologyNode = root_topologyNode.get_node_by_name("salaries")
# print(salaries_source_topologyNode.output_handle_function()().get())
salaries_source_topologyNode.output_handle_function()().get().send(salary_zset1)
# print(salaries_source_topologyNode.output_handle_function()().get())

root_topologyNode.step()

print()
print(f"Latest: {root_topologyNode.latest()}")
