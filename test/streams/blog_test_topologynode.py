from kafi.streams.topologynode import TopologyNode as Tn

#

t1 = "employees"
t2 = "salaries"
#
employee_message_dict_list = [{"key": "0", "value": {"id": 0, "name": "kristjan"}},
                            {"key": "1", "value": {"id": 1, "name": "mark"}},
                            {"key": "2", "value": {"id": 2, "name": "mike"}}]
salary_message_dict_list = [{"key": "2", "value": {"id": 2, "salary": 40000}},
                            {"key": "0", "value": {"id": 0, "salary": 38750}},
                            {"key": "1", "value": {"id": 1, "salary": 50000}}]
#
def get_root_tn():
    employees_source_tn = Tn.source(t1)
    salaries_source_tn = Tn.source(t2)
    #
    root_tn = (
        employees_source_tn
        .project(lambda x: {"id": x["value"]["id"], "name": x["value"]["name"]})
        .select(lambda x: x["name"] != "mark")
        .join_equi(
            salaries_source_tn
            .project(lambda x: {"id": x["value"]["id"], "salary": x["value"]["salary"]}),
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
root_tn = get_root_tn()
#
root_tn.push(t1, employee_message_dict_list)
root_tn.push(t2, salary_message_dict_list)
#
x = root_tn.step()
print(x)
#
root_tn.push(t2, [{"key": "0", "value": {"id": 0, "salary": 100000}}])
#
x = root_tn.step()
print(x)
