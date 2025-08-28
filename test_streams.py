import asyncio
from kafi.kafi import *
# c = Cluster("local")
c = Local("local")
t1 = "employees"
t2 = "salaries"
t3 = "sink"
c.retouch(t1)
c.retouch(t2)
c.retouch(t3)
print(c.l())
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
def sel(message_dict):
    message_dict["value"]["name"] = message_dict["value"]["name"] + "_abc"
    return message_dict

def proj(_, left_message_dict, right_message_dict):
    left_message_dict["value"].update(right_message_dict["value"])
    return left_message_dict

topology = (
    employees_source
    .where(lambda message_dict: message_dict["value"]["name"] != "mark")
    .join(
        salaries_source,
        on=lambda message_dict: message_dict["key"],
        projection=proj
    )
    .select(sel)
)
print(topology.topology())
#
async def start():
    await run([(c, employees_source), (c, salaries_source)], c, t3, topology)
asyncio.run(start())
#
print(c.cat(t3))
