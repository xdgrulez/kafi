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

def agg_function(group_any_zset_tuple):
    agg_group_any_message_dict = {}
    for group_any, zset in group_any_zset_tuple:
        for message_dict, weight_int in zset.items():
            if group_any not in agg_group_any_message_dict:
                message_dict["value"]["amount"] = message_dict["value"]["amount"] * weight_int
                agg_group_any_message_dict[group_any] = message_dict
            else:
                message_dict["value"]["amount"] += agg_group_any_message_dict[group_any]["value"]["amount"] * weight_int
                agg_group_any_message_dict[group_any] = message_dict
    #
    return ZSet({(group_any, message_dict): 1 for group_any, message_dict in agg_group_any_message_dict.items()})


def setup():
    employees_source_topologyNode = source("employees")
    salaries_source_topologyNode = source("salaries")
    #
    root_topologyNode = (
        employees_source_topologyNode
        .filter(lambda message_dict: message_dict["value"]["name"] != "mark")
        .join(
            salaries_source_topologyNode,
            on_function=on_function,
            projection_function=proj_function
        )
        # .peek(print)
        .map(map_function)
    )
    #
    return employees_source_topologyNode, salaries_source_topologyNode, root_topologyNode

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
