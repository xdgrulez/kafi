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

from copy import deepcopy

import cloudpickle as pickle

import datetime, gc, os, random, sys, time

import objgraph

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

from kafi.streams.topologynode import agg_fun, as_fun, group_by_agg_fun, message_dict_list_to_ZSet, select_fun, select_as_fun, source

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
    root_topologyNode = (
        transactions_source_topologyNode
        .agg(agg_fun([(sum_fun, select_fun(["amount"]), select_as_fun(["sum"]))]), False)
        # .group_by_agg(select_fun(["id"]), group_by_agg_fun([(sum_fun, select_fun(["amount"]), select_as_fun(["sum"]), as_fun(["id"]))]))
    )

    return transactions_source_topologyNode, root_topologyNode

#

# pr = cProfile.Profile()
# pr.enable()

#

# tracemalloc.start()

#

def transactions(transactions_source_topologyNode, root_topologyNode, n_int, i):
    random.seed(42)
    transactions_message_dict_list = []
    for id_int in range(i, i + n_int):
        message_dict = {"key": str(id_int),
                        "value": {
                            "id": id_int,
                                    # "from_account": random.randint(0, 9),
                                    # "to_account": random.randint(0, 9),
                                    "amount": 1}}
                                    # "ts": datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")}}
        transactions_message_dict_list.append(message_dict)
    #
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
    return id_int

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

def print_biggest_attrs(obj, limit=3):
    attrs = []
    for attr in dir(obj):
        try:
            val = getattr(obj, attr)
            attrs.append((attr, asizeof.asizeof(val, code=True)))
        except:
            continue
    
    attrs.sort(key=lambda x: x[1], reverse=True)
    for name, size in attrs[:limit]:
        print(f"  {name}: {size / 1024:.2f} KB")

# start_time = time.time()
last_id_int = 0
for i in range(100):
    # start_time1 = time.time()
    print(i)
    # obj_report = asizeof.asized(root_topologyNode, detail=1, code=True)
    # print(obj_report.size)

    # print(f"Code-Objekte davor: {count_runtime_objects()}")
    # last_id_int = transactions(transactions_source_topologyNode, root_topologyNode, 10000, last_id_int)
    last_id_int = transactions(transactions_source_topologyNode, root_topologyNode, 10000, 0)
    # print(f"Code-Objekte danach: {count_runtime_objects()}")
    # end_time1 = time.time()
    # print(end_time1 - start_time1)

    print()
    print(f"Latest: {root_topologyNode.latest()}")
    # print(transactions_source_topologyNode.output_handle_function().get().inner.keys())
    # print(len(pickle.dumps(transactions_source_topologyNode)) / 1024 / 1024)
    print(len(pickle.dumps(root_topologyNode)) / 1024 / 1024)
    # print_biggest_attrs(root_topologyNode)

# objgraph.show_growth()

#
# end_time = time.time()
# print(end_time - start_time)
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

# print_biggest_attrs(root_topologyNode)

# print(root_topologyNode.output_handle_function().get())
