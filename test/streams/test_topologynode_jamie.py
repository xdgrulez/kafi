# >>> x = (2, 49)
# >>> y = (10, 69)
# >>> z = (20, 93)
# 30, 119
# 2.5*20 + 44 

# 2, 49
# 10, 65
# 20, 82
# 30, 101
# 1.9*20 + 44 

import cloudpickle as pickle

import datetime, gc, os, random, sys, time

# import objgraph

# import cProfile, pstats, io
# from pstats import SortKey

import tracemalloc

import gc, types

from pympler import asizeof

from pympler import muppy, summary
#

if os.path.basename(os.getcwd()) == "kafi":
    sys.path.insert(1, ".")
else:
    sys.path.insert(1, "../..")

from kafi.streams.topologynode import agg_tuple, message_dict_list_to_ZSet, source

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

def sum_fun(i, j):
    return i + j

#

def setup():
    transactions_source_topologyNode = source("transactions")
    #
# create view credits as select to_account as account, sum(amount) as credits from transactions group by to_account;
    credits_topologyNode = (
        transactions_source_topologyNode
        .group_by_agg(by_function_list=[lambda x: x["to_account"]],
                      as_function=lambda x, y: x.update({"account": y}),
                      agg_tuple_list=[agg_tuple(select_function=lambda x: x["amount"],
                                                agg_function=lambda x, y: x + y,
                                                as_function=lambda x, y: x.update({"credits": y}))])
    )
    
# create view debits as select from_account as account, sum(amount) as debits from transactions group by from_account;
    debits_topologyNode = (
        transactions_source_topologyNode
        .group_by_agg(by_function_list=[lambda x: x["from_account"]],
                      as_function=lambda x, y: x.update({"account": y}),
                      agg_tuple_list=[agg_tuple(select_function=lambda x: x["amount"],
                                                agg_function=lambda x, y: x + y,
                                                as_function=lambda x, y: x.update({"debits": y}))])
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
        .agg(agg_tuple_list=[agg_tuple(select_function=lambda x: x["balance"], 
                                       agg_function=lambda x, y: x + y,
                                       as_function=lambda x, y: x.update({"sum": y}))])
    )
    #
    return transactions_source_topologyNode, root_topologyNode

#

# pr = cProfile.Profile()
# pr.enable()

#

# tracemalloc.start()

#

def transactions(transactions_source_topologyNode, root_topologyNode):
    n_int = 10000
    random.seed(42)
    transactions_message_dict_list = []
    for id_int in range(0, n_int):
        message_dict = {"key": str(id_int),
                        "value": {
                            # "id": id_int,
                                    "from_account": random.randint(0, 9),
                                    "to_account": random.randint(0, 9),
                                    "amount": 1}}
                                    # "ts": datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")}}
        transactions_message_dict_list.append(message_dict)
    #
    # transactions_message_dict_list = [{'key': '0', 'value': {'from_account': 1, 'to_account': 0, 'amount': 1}}, {'key': '1', 'value': {'from_account': 4, 'to_account': 3, 'amount': 1}}, {'key': '2', 'value': {'from_account': 3, 'to_account': 2, 'amount': 1}}, {'key': '3', 'value': {'from_account': 1, 'to_account': 8, 'amount': 1}}, {'key': '4', 'value': {'from_account': 1, 'to_account': 9, 'amount': 1}}, {'key': '5', 'value': {'from_account': 6, 'to_account': 0, 'amount': 1}}, {'key': '6', 'value': {'from_account': 0, 'to_account': 1, 'amount': 1}}, {'key': '7', 'value': {'from_account': 3, 'to_account': 3, 'amount': 1}}, {'key': '8', 'value': {'from_account': 8, 'to_account': 9, 'amount': 1}}, {'key': '9', 'value': {'from_account': 0, 'to_account': 8, 'amount': 1}}]
    transactions_zset = message_dict_list_to_ZSet(transactions_message_dict_list)
    # x = list(transactions_zset.inner.keys())[0]
    # print(sys.getrefcount(x))
    # print(gc.get_referrers(x))
    # objgraph.show_backrefs(x, max_depth=2)
    transactions_source_topologyNode.output_handle_function().get().send(transactions_zset)
    #
    root_topologyNode.step()
    root_topologyNode.gc()

#

transactions_source_topologyNode, root_topologyNode = setup()

#

def count_runtime_objects():
    counts = {"code": 0, "function": 0, "lambda": 0}
    for obj in gc.get_objects():
        if isinstance(obj, types.CodeType):
            counts["code"] += 1
        if isinstance(obj, types.FunctionType):
            counts["function"] += 1
            if obj.__name__ == "<lambda>":
                counts["lambda"] += 1
    return counts

start_time = time.time()
for i in range(100):
    # start_time1 = time.time()
    print(i)
    # obj_report = asizeof.asized(root_topologyNode, detail=1, code=True)
    # print(obj_report.size)

    # print(f"Code-Objekte davor: {count_runtime_objects()}")
    transactions(transactions_source_topologyNode, root_topologyNode)
    # print(f"Code-Objekte danach: {count_runtime_objects()}")
    # end_time1 = time.time()
    # print(end_time1 - start_time1)

    # del transactions_source_topologyNode.output_handle_function().get().inner[i + 1]

    print()
    print(f"Latest: {root_topologyNode.latest()}")
    print(len(pickle.dumps(root_topologyNode)) / 1024 / 1024)
#
end_time = time.time()
print(end_time - start_time)
#

# print()
# print(f"Topology: {root_topologyNode.topology()}")
# print()
# print(f"Mermaid:\n{root_topologyNode.mermaid()}")
# print()
# print(f"Topology: {root_topologyNode.topology(True)}")
# print()
# print(f"Mermaid:\n{root_topologyNode.mermaid(True)}")
# print()
# print(f"Latest: {root_topologyNode.latest()}")

# pr.disable()
# s = io.StringIO()
# sortby = SortKey.CUMULATIVE
# ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
# ps.print_stats()
# print(s.getvalue())

# snapshot = tracemalloc.take_snapshot()
# top_stats = snapshot.statistics('lineno')

# print("[ Top 10 Allocation Sites ]")
# for stat in top_stats[:10]:
#     print(stat)
    
#

# current, peak = tracemalloc.get_traced_memory()
# print(f"Current: {current / 10**6} MB; Peak: {peak / 10**6} MB")
# tracemalloc.stop()

# print(asizeof.asizeof(root_topologyNode, code=True) / 1024 / 1024)

# all_objs = muppy.get_objects()
# sum1 = summary.summarize(all_objs)
# summary.print_(sum1)

def print_biggest_attrs(obj, limit=20):
    attrs = []
    for attr in dir(obj):
        try:
            val = getattr(obj, attr)
            attrs.append((attr, asizeof.asizeof(val)))
        except:
            continue
    
    attrs.sort(key=lambda x: x[1], reverse=True)
    for name, size in attrs[:limit]:
        print(f"  {name}: {size / 1024:.2f} KB")

# print_biggest_attrs(root_topologyNode)

# print(root_topologyNode.output_handle_function().get())
