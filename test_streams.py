from kafi.kafi import *
l = Local("local")
t1 = "employees"
t2 = "salaries"
t3 = "sink"
l.retouch(t1)
l.retouch(t2)
l.retouch(t3)
#
employee_message_dict_list = [{"key": "0", "value": {"name": "kristjan"}},
                            {"key": "1", "value": {"name": "mark"}},
                            {"key": "2", "value": {"name": "mike"}}]
salary_message_dict_list = [{"key": "2", "value": {"salary": 40000}},
                            {"key": "0", "value": {"salary": 38750}},
                            {"key": "1", "value": {"salary": 50000}}]
#
pr = l.producer(t1)
for x in employee_message_dict_list:
    pr.produce(x["value"], key=x["key"])
pr.close()
#
pr = l.producer(t2)
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
#
run([(l, employees_source), (l, salaries_source)], l, t3, topology)
#
print(l.cat(t3))
