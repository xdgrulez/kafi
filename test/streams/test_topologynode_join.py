import os, sys

import cloudpickle as pickle

#

if os.path.basename(os.getcwd()) == "kafi":
    sys.path.insert(1, ".")
else:
    sys.path.insert(1, "../..")

from kafi.streams.topologynode import source, message_dict_list_to_ZSet

#

def setup():
    employees_source_topologyNode1 = source("employees1")
    employees_source_topologyNode2 = source("employees2")
    #
    root_topologyNode = (
        employees_source_topologyNode1
        .join(employees_source_topologyNode2,
              on_function=lambda l, r: l["name"] == r["name"],
              projection_function=lambda l, r: {"name": l["name"]})
    )
    #
    return employees_source_topologyNode1, employees_source_topologyNode2, root_topologyNode

employees_source_topologyNode1, employees_source_topologyNode2, root_topologyNode = setup()

employee_message_dict_list1 = [{"key": "0", "value": {"name": "kristjan"}},
                               {"key": "1", "value": {"name": "mark"}}]
employee_message_dict_list2 = [{"key": "0", "value": {"name": "kristjan"}}]

employee_zset1 = message_dict_list_to_ZSet(employee_message_dict_list1)
employee_zset2 = message_dict_list_to_ZSet(employee_message_dict_list2)
employees_source_topologyNode1.output_handle_function().get().send(employee_zset1)
employees_source_topologyNode2.output_handle_function().get().send(employee_zset2)

root_topologyNode.step()

# print()
# print(f"Topology: {root_topologyNode.topology()}")
# print()
# print(f"Mermaid:\n{root_topologyNode.mermaid()}")
# print()
# print(f"Topology: {root_topologyNode.topology(True)}")
# print()
# print(f"Mermaid:\n{root_topologyNode.mermaid(True)}")
print()
print(f"Latest: {root_topologyNode.latest()}")

del employees_source_topologyNode1.output_handle_function().get().inner[1]
del employees_source_topologyNode2.output_handle_function().get().inner[1]

employee_message_dict_list21 = [{"key": "1", "value": {"name": "mark"}}]
employee_zset21 = message_dict_list_to_ZSet(employee_message_dict_list21)
employees_source_topologyNode2.output_handle_function().get().send(employee_zset21)

root_topologyNode.step()

print()
print(f"Latest: {root_topologyNode.latest()}")
