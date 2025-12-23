import os, sys

import cloudpickle as pickle

#

if os.path.basename(os.getcwd()) == "kafi":
    sys.path.insert(1, ".")
else:
    sys.path.insert(1, "../..")

from kafi.streams.topologynode import source, message_dict_list_to_ZSet

def map_function(value_dict):
    value_dict["name"] = value_dict["name"] + "_abc"
    return value_dict

def on_function(left_value_dict, right_value_dict):
    return left_value_dict["_key"] == right_value_dict["_key"]

def proj_function(left_value_dict, right_value_dict):
    left_value_dict.update(right_value_dict)
    return left_value_dict

def setup():
    employees_source_topologyNode = source("employees")
    salaries_source_topologyNode = source("salaries")
    #
    root_topologyNode = (
        employees_source_topologyNode
        .filter(lambda value_dict: value_dict["name"] != "mark")
        .join(
            salaries_source_topologyNode,
            on_function=on_function,
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
print(f"Topology: {root_topologyNode.topology(True)}")
print()
print(f"Mermaid:\n{root_topologyNode.mermaid(True)}")
print()
print(f"Latest: {root_topologyNode.latest()}")

#

root_topologyNode = pickle.loads(pickle.dumps(root_topologyNode))

salary_message_dict_list1 = [{"key": "0", "value": {"salary": 100000}}]
salary_zset1 = message_dict_list_to_ZSet(salary_message_dict_list1)

# salaries_source_topologyNode = root_topologyNode.get_node_by_id(salaries_source_topologyNode.id())
salaries_source_topologyNode = root_topologyNode.get_node_by_name("salaries")
# print(salaries_source_topologyNode.output_handle_function()().get())
salaries_source_topologyNode.output_handle_function()().get().send(salary_zset1)
# print(salaries_source_topologyNode.output_handle_function()().get())

root_topologyNode.step()

print()
print(f"Latest: {root_topologyNode.latest()}")
