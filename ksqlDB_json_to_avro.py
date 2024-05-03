import json
from kafi.kafi import *
#c = Cluster("azureq")
c = Local("local_azureq_sr")
#
t = "mgb.dev.data.human-resources.time-management.TimeType"
#
sr = c.schemaRegistry
key_schema_str = sr.get_latest_version(t + "-key")["schema"]["schema_str"]
value_schema_str = sr.get_latest_version(t + "-value")["schema"]["schema_str"]
key_schema_dict = json.loads(key_schema_str)
value_schema_dict = json.loads(value_schema_str)
#
def get_avro_fields(x, acc_field_str_list):
    if isinstance(x, dict):
        if "name" in x:
            acc_field_str_list += [x["name"]]
        field_str_lists = [get_avro_fields(value, []) for value in x.values()]
        field_str_list = [field_str for field_str_list in field_str_lists for field_str in field_str_list]
        acc_field_str_list += field_str_list
    elif isinstance(x, list):
        field_str_lists = [get_avro_fields(list_item, []) for list_item in x]
        field_str_list = [field_str for field_str_list in field_str_lists for field_str in field_str_list]
        acc_field_str_list += field_str_list
    return acc_field_str_list
#
def _map_upper_to_avro(x, mapping_dict):
    if isinstance(x, dict):
        return {mapping_dict[key]: _map_upper_to_avro(value, mapping_dict) for key, value in x.items()}
    elif isinstance(x, list):
        return [_map_upper_to_avro(list_item, mapping_dict) for list_item in x]
    else:
        return x

def map_upper_to_avro(message_dict):
    message_dict["key"] = _map_upper_to_avro(message_dict["key"], upper_key_field_str_avro_key_field_str_dict)
    message_dict["value"] = _map_upper_to_avro(message_dict["value"], upper_value_field_str_avro_value_field_str_dict)
    return message_dict
#
avro_key_field_str_list = get_avro_fields(key_schema_dict, [])
avro_value_field_str_list = get_avro_fields(value_schema_dict, [])

#print(avro_value_field_str_list)

upper_key_field_str_avro_key_field_str_dict = {avro_key_field_str.upper(): avro_key_field_str for avro_key_field_str in avro_key_field_str_list}
upper_value_field_str_avro_value_field_str_dict = {avro_value_field_str.upper(): avro_value_field_str for avro_value_field_str in avro_value_field_str_list}

c.cp(t + "_json", c, t, source_type="json", target_type="avro", target_key_schema=key_schema_str, target_value_schema=value_schema_str, map_function=map_upper_to_avro)
