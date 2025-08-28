import asyncio
import threading
from kafi.kafi import *
# c = Cluster("local")
c = Local("local")
t1 = "employees"
t2 = "salaries"
t3 = "sink"
c.retouch(t1)
c.retouch(t2)
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
for x in employee_message_dict_list:
    pr.produce(x["value"], key=x["key"])
pr.close()
#
pr = c.producer(t2)
for x in salary_message_dict_list:
    pr.produce(x["value"], key=x["key"])
pr.close()
#
employees_source = source(t1)
salaries_source = source(t2)
#
def map_fun(message_dict):
    message_dict["value"]["name"] = message_dict["value"]["name"] + "_abc"
    return message_dict

def proj_fun(_, left_message_dict, right_message_dict):
    left_message_dict["value"].update(right_message_dict["value"])
    return left_message_dict

topology = (
    employees_source
    .filter(lambda message_dict: message_dict["value"]["name"] != "mark")
    .join(
        salaries_source,
        on_function=lambda message_dict: message_dict["key"],
        projection_function=proj_fun
    )
    .peek(print)
    .map(map_fun)
)
print(topology.topology())
#
async def test():
    def run():
        asyncio.run(streams([(c, employees_source), (c, salaries_source)], topology, c, t3))
    #
    thread = threading.Thread(target=run)
    thread.daemon = True
    thread.start()
    #
    await asyncio.sleep(8)
    #
    print(c.l())
    print(c.cat(t3))
    #
    pr = c.producer(t2)
    pr.produce({"salary": 100000}, key="0")
    pr.close()
    #
    await asyncio.sleep(2)
    #
    print(c.l())
    print(c.cat(t3))
#
asyncio.run(test())
