import datetime, os, random, sys

# import cProfile, pstats, io
# from pstats import SortKey

import tracemalloc

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
# create view credits as select to_account as account, sum(amount) as credits from transactions group by to_account;
    credits_topologyNode = (
        transactions_source_topologyNode
        .group_by_agg(select_fun(["to_account"]), group_by_agg_fun([(sum_fun, select_fun(["amount"]), select_as_fun(["credits"]), as_fun(["account"]))]))
    )
    
# create view debits as select from_account as account, sum(amount) as debits from transactions group by from_account;
    debits_topologyNode = (
        transactions_source_topologyNode
        .group_by_agg(select_fun(["from_account"]), group_by_agg_fun([(sum_fun, select_fun(["amount"]), select_as_fun(["debits"]), as_fun(["account"]))]))
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
        .agg(agg_fun([(sum_fun, select_fun(["balance"]), select_as_fun(["sum"]))]))
    )

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
                        "value": {"id": id_int,
                                    "from_account": random.randint(0, 9),
                                    "to_account": random.randint(0, 9),
                                    "amount": 1,
                                    "ts": datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")}}
        transactions_message_dict_list.append(message_dict)
    #
    transactions_zset = message_dict_list_to_ZSet(transactions_message_dict_list)
    transactions_source_topologyNode.output_handle_function().get().send(transactions_zset)
    #
    root_topologyNode.step()

#

transactions_source_topologyNode, root_topologyNode = setup()

#

for i in range(3):
    transactions(transactions_source_topologyNode, root_topologyNode)
    print()
    print(f"Latest: {root_topologyNode.latest()}")

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

# from pympler import asizeof

# print(asizeof.asizeof(root_topologyNode, code=-1, stats=1))

# all_objs = muppy.get_objects()
# sum1 = summary.summarize(all_objs)
# summary.print_(sum1)

# def print_biggest_attrs(obj, limit=20):
#     attrs = []
#     for attr in dir(obj):
#         try:
#             val = getattr(obj, attr)
#             attrs.append((attr, asizeof.asizeof(val)))
#         except:
#             continue
    
#     attrs.sort(key=lambda x: x[1], reverse=True)
#     for name, size in attrs[:limit]:
#         print(f"  {name}: {size / 1024:.2f} KB")

# print_biggest_attrs(root_topologyNode)

# print(root_topologyNode.output_handle_function().get())
