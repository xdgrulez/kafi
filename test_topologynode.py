from kafi.streams.streams import *

employee_message_dict_list = [{"key": "0", "value": {"name": "kristjan"}},
                            {"key": "1", "value": {"name": "mark"}},
                            {"key": "2", "value": {"name": "mike"}}]
salary_message_dict_list = [{"key": "2", "value": {"salary": 40000}},
                            {"key": "0", "value": {"salary": 38750}},
                            {"key": "1", "value": {"salary": 50000}}]

def map_fun(message_dict):
    message_dict["value"]["name"] = message_dict["value"]["name"] + "_abc"
    return message_dict

def proj_fun(_, left_message_dict, right_message_dict):
    left_message_dict["value"].update(right_message_dict["value"])
    return left_message_dict

employees_source_topologyNode = source("employees_source")
salaries_source_topologyNode = source("salaries_source")

root_topologyNode = (
    employees_source_topologyNode
    .filter(lambda message_dict: message_dict["value"]["name"] != "mark")
    .join(
        salaries_source_topologyNode,
        on_function=lambda message_dict: message_dict["key"],
        projection_function=proj_fun
    )
    .peek(print)
    .map(map_fun)
)

employee_zset = message_dict_list_to_zset(employee_message_dict_list)
salary_zset = message_dict_list_to_zset(salary_message_dict_list)
employees_source_topologyNode.stream_handle().get().send(employee_zset)
salaries_source_topologyNode.stream_handle().get().send(salary_zset)

root_topologyNode.step()

print()
print(f"Topology: {root_topologyNode.topology()}")
print()
print(f"Latest: {root_topologyNode.latest()}")
