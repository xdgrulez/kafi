import os, sys

#

if os.path.basename(os.getcwd()) == "kafi":
    sys.path.insert(1, ".")
else:
    sys.path.insert(1, "../..")

from kafi.streams.topologynode import source, message_dict_list_to_ZSet

#

def setup():
    employees_source_topologyNode = source("employees")
    #
    root_topologyNode = (
        employees_source_topologyNode
        .map(lambda x: x["name"] + "_abc")
    )
    #
    return employees_source_topologyNode, root_topologyNode

employees_source_topologyNode, root_topologyNode = setup()

employee_message_dict_list = [{"key": "0", "value": {"name": "kristjan"}},
                              {"key": "1", "value": {"name": "mark"}},
                              {"key": "2", "value": {"name": "mike"}}]

employee_zset = message_dict_list_to_ZSet(employee_message_dict_list)
employees_source_topologyNode.output_handle_function()().get().send(employee_zset)

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
