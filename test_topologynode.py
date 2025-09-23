from kafi.streams.streams import *

def map_function(message_dict):
    message_dict["value"]["name"] = message_dict["value"]["name"] + "_abc"
    return message_dict

def proj_function(_, left_message_dict, right_message_dict):
    left_message_dict["value"].update(right_message_dict["value"])
    return left_message_dict

def setup():
    employees_source_topologyNode = source("employees_source")
    salaries_source_topologyNode = source("salaries_source")
    #
    root_topologyNode = (
        employees_source_topologyNode
        .filter(lambda message_dict: message_dict["value"]["name"] != "mark")
        .join(
            salaries_source_topologyNode,
            on_function=lambda message_dict: message_dict["key"],
            projection_function=proj_function
        )
        # .peek(print)
        .map(map_function)
    )
    #
    return employees_source_topologyNode, salaries_source_topologyNode, root_topologyNode

employees_source_topologyNode, salaries_source_topologyNode, root_topologyNode = setup()

employee_message_dict_list = [{"key": "0", "value": {"name": "kristjan"}},
                            {"key": "1", "value": {"name": "mark"}},
                            {"key": "2", "value": {"name": "mike"}}]
salary_message_dict_list = [{"key": "2", "value": {"salary": 40000}},
                            {"key": "0", "value": {"salary": 38750}},
                            {"key": "1", "value": {"salary": 50000}}]

employee_zset = message_dict_list_to_ZSet(employee_message_dict_list)
salary_zset = message_dict_list_to_ZSet(salary_message_dict_list)
employees_source_topologyNode.output_handle_function()().get().send(employee_zset)
salaries_source_topologyNode.output_handle_function()().get().send(salary_zset)

root_topologyNode.step()

print()
print(f"Topology: {root_topologyNode.topology()}")
print()
print(f"Mermaid:\n{root_topologyNode.mermaid()}")
print()
print(f"Latest: {root_topologyNode.latest()}")

#

salary_message_dict_list1 = [{"key": "0", "value": {"salary": 100000}}]
salary_zset1 = message_dict_list_to_ZSet(salary_message_dict_list1)

salaries_source_topologyNode.output_handle_function()().get().send(salary_zset1)

root_topologyNode.step()

print()
print(f"Latest: {root_topologyNode.latest()}")
