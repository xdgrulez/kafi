import asyncio, os, sys, threading

#

if os.path.basename(os.getcwd()) == "kafi":
    sys.path.insert(1, ".")
else:
    sys.path.insert(1, "../..")

from kafi.kafi import *

#

# c = Cluster("local")
c = Cluster("local")
t1 = "employees"
t2 = "salaries"
t3 = "sink"
t4 = "snapshot"
g1 = f"group_{get_millis()}"
c.retouch(t1, partitions=2)
c.retouch(t2, partitions=2)
c.retouch(t3)
c.retouch(t4)
#
employee_message_dict_list = [{"key": "0", "value": {"id": 0, "name": "kristjan"}},
                            {"key": "1", "value": {"id": 1, "name": "mark"}},
                            {"key": "2", "value": {"id": 2, "name": "mike"}}]
salary_message_dict_list = [{"key": "2", "value": {"id": 2, "salary": 40000}},
                            {"key": "0", "value": {"id": 0, "salary": 38750}},
                            {"key": "1", "value": {"id": 1, "salary": 50000}}]
#
pr = c.producer(t1)
partition_int = 0
for x in employee_message_dict_list:
    pr.produce(x["value"], key=x["key"], partition=partition_int)
    partition_int = 1 if partition_int == 0 else 0
pr.close()
#
pr = c.producer(t2)
partition_int = 0
for x in salary_message_dict_list:
    pr.produce(x["value"], key=x["key"], partition=partition_int)
    partition_int = 1 if partition_int == 0 else 0
pr.close()
#
def get_root_tn():
    employees_source_tn = source(t1)
    salaries_source_tn = source(t2)
    #
    root_tn = (
        employees_source_tn
        .map(lambda x: {"id": x["value"]["id"], "name": x["value"]["name"]})
        .filter(lambda x: x["name"] != "mark")
        .join_equi(
            salaries_source_tn
            .map(lambda x: {"id": x["value"]["id"], "salary": x["value"]["salary"]}),
            lambda x: x["id"],
            lambda x: x["id"],
            lambda l, r: {"value": {"id": l["id"], "name": l["name"] + "_abc", "salary": r["salary"]}}
        )
    )
    #
    root_tn.build()
    #
    return root_tn
#
async def test():
    group_str = "test_streams_checkpointing"
    
    print(c.l())
    print(c.cat(t1))
    print(c.cat(t2))
    print()

    def run(stop_thread=None):
        asyncio.run(streams([(c, t1), (c, t2)], get_root_tn(), c, t3, c, t4, stop_thread, group=g1))
    #
    stop_thread = threading.Event()
    thread = threading.Thread(target=run, args=[stop_thread])
    thread.daemon = True
    thread.start()
    print("Started streams thread.")
    #
    await asyncio.sleep(8)
    #
    print()
    print(c.l())
    print(c.cat(t3, group=group_str))
    print()
    #
    stop_thread.set()
    thread.join()
    print("Stopped streams thread.")
    #
    thread = threading.Thread(target=run)
    thread.daemon = True
    thread.start()
    print("Started streams thread again.")
    #
    pr = c.producer(t2)
    pr.produce({"id": 0, "salary": 100000}, key="0")
    pr.close()
    #
    await asyncio.sleep(8)
    #
    print()
    print(c.l())
    print(c.cat(t3, group=group_str))
    #
    thread.join()
    print("Stopped streams thread again.")
#
asyncio.run(test())
