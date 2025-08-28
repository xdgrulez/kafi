import asyncio
import threading
from kafi.kafi import *
# c = Cluster("local")
c = Local("local")
t1 = "employees"
t2 = "salaries"
t3 = "sink"
c.retouch(t1, partitions=2)
c.retouch(t2, partitions=2)
c.retouch(t3)
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
employees_source_topologyNode = source(t1)
salaries_source_topologyNode = source(t2)
#
def map_fun(message_dict):
    message_dict["value"]["name"] = message_dict["value"]["name"] + "_abc"
    return message_dict

def proj_fun(_, left_message_dict, right_message_dict):
    left_message_dict["value"].update(right_message_dict["value"])
    return left_message_dict

root_topologyNode = (
    employees_source_topologyNode
    .filter(lambda message_dict: message_dict["value"]["name"] != "mark")
    .join(
        salaries_source_topologyNode,
        on_fun=lambda message_dict: message_dict["key"],
        projection_fun=proj_fun
    )
    # .peek(print)
    .map(map_fun)
)
print(root_topologyNode.topology())
#
async def test():
    print(c.cat(t1))
    print(c.cat(t2))
    print()

    def run():
        asyncio.run(streams([(c, employees_source_topologyNode), (c, salaries_source_topologyNode)], root_topologyNode, c, t3))
    #
    thread = threading.Thread(target=run)
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
    pr = c.producer(t2)
    pr.produce({"salary": 100000}, key="0")
    pr.close()
    #
    await asyncio.sleep(2)
    #
    print()
    print(c.l())
    print(c.cat(t3))
#
asyncio.run(test())
