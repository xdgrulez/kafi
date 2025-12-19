import asyncio, os, sys, threading

#

if os.path.basename(os.getcwd()) == "kafi":
    sys.path.insert(1, ".")
else:
    sys.path.insert(1, "../..")

from kafi.kafi import *

#

# c = Cluster("local")
c = Local("local")
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
employee_message_dict_list = [{"key": "0", "value": {"name": "kristjan"}},
                            {"key": "1", "value": {"name": "mark"}},
                            {"key": "2", "value": {"name": "mike"}}]
salary_message_dict_list = [{"key": "2", "value": {"salary": 40000}},
                            {"key": "0", "value": {"salary": 38750}},
                            {"key": "1", "value": {"salary": 50000}}]
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
def map_function(message_dict):
    message_dict["value"]["name"] = message_dict["value"]["name"] + "_abc"
    return message_dict

def on_function(left_message_dict, right_message_dict):
    return left_message_dict["key"] == right_message_dict["key"]

def proj_function(left_message_dict, right_message_dict):
    left_message_dict["value"].update(right_message_dict["value"])
    return left_message_dict

def get_root_topologyNode():
    employees_source_topologyNode = source(t1)
    salaries_source_topologyNode = source(t2)
    #
    root_topologyNode = (
        employees_source_topologyNode
        .filter(lambda message_dict: message_dict["value"]["name"] != "mark")
        .join(
            salaries_source_topologyNode,
            on_function=on_function,
            projection_function=proj_function
        )
        .peek(print)
        .map(map_function)
    )
    #
    return root_topologyNode
#
async def test():
    print(c.l())
    print(c.cat(t1))
    print(c.cat(t2))
    print()

    def run(stop_thread=None):
        asyncio.run(streams([(c, t1), (c, t2)], get_root_topologyNode(), c, t3, c, t4, stop_thread, group=g1))
    #
    stop_thread = threading.Event()
    thread = threading.Thread(target=run, args=[stop_thread])
    thread.daemon = True
    thread.start()
    #
    await asyncio.sleep(8)
    #
    print()
    print(c.l())
    print(c.cat(t3))
    print()
    #
    stop_thread.set()
    thread.join()
    
    thread = threading.Thread(target=run)
    thread.daemon = True
    thread.start()
    #
    pr = c.producer(t2)
    pr.produce({"salary": 100000}, key="0")
    pr.close()
    #
    await asyncio.sleep(8)
    #
    print()
    print(c.l())
    print(c.cat(t3))
    #
    thread.join()
#
asyncio.run(test())
