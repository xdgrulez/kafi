import base64
import binascii
import datetime
import dateutil.parser
from fnmatch import fnmatch
from functools import reduce
import json
# import jsonpath_ng
# import logging
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import sys
import time

# Constants

RD_KAFKA_PARTITION_UA = -1

#

def get_millis():
    return int(time.time()*1000)


def to_millis(timestamp_str):
    return int(dateutil.parser.isoparse(timestamp_str).timestamp()*1000)


def from_millis(millis_int):
    return datetime.datetime.fromtimestamp(millis_int/1000.0).isoformat(sep=" ")


def is_interactive():
    return hasattr(sys, 'ps1')


def pretty(dict):
    return json.dumps(dict, indent=2, default=str)


def ppretty(dict):
    print(pretty(dict))

#

def create_session(retries_int):
    # logging.basicConfig(level=logging.DEBUG)
    session = requests.Session()
    retry = Retry(total=retries_int, backoff_factor=2, status_forcelist=[500, 502, 503, 504], allowed_methods=None)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    #
    return session


def get(url_str, headers_dict, payload_dict=None, auth_str_tuple=None, retries_int=0, debug_bool=False):
    session = create_session(retries_int)
    #
    if payload_dict is None:
        if debug_bool:
            print(f"GET Request\n-\nURL: {url_str}\nHeaders: {headers_dict}\n")
        #
        response = session.get(url_str, headers=headers_dict, auth=auth_str_tuple)
    else:
        if debug_bool:
            print(f"GET Request\n-\nURL: {url_str}\nHeaders: {headers_dict}\nPayload: {payload_dict}")
        #
        response = session.get(url_str, headers=headers_dict, json=payload_dict, auth=auth_str_tuple)
    #
    if debug_bool:
        print(f"GET Response\n-\n{response.text}\n")
    #
    if is_json(response.text):
        response_dict = response.json()
    else:
        response_dict = {"response": response.text}
    #
    if isinstance(response_dict, dict):
        if "error_code" in response_dict and response_dict["error_code"] > 400:
            raise Exception(response_dict["message"])
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)


def delete(url_str, headers_dict, auth_str_tuple=None, retries_int=10, debug_bool=False):
    session = create_session(retries_int)
    #
    if debug_bool:
        print(f"DELETE Request\n-\nURL: {url_str}\nHeaders: {headers_dict}\n")
    #
    response = session.delete(url_str, headers=headers_dict, auth=auth_str_tuple)
    #
    if debug_bool:
        print(f"DELETE Response\n-\n{response.text}\n")
    #
    if is_json(response.text):
        response_dict = response.json()
    else:
        response_dict = {"response": response.text}
    #
    if isinstance(response_dict, dict):
        if "error_code" in response_dict and response_dict["error_code"] > 400:
            raise Exception(response_dict["message"])
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)


def post(url_str, headers_dict, payload_dict_or_generator, auth_str_tuple=None, retries_int=10, debug_bool=False):
    session = create_session(retries_int)
    #
    if isinstance(payload_dict_or_generator, dict):
        if debug_bool:
            print(f"POST Request\n-\nURL: {url_str}\nHeaders: {headers_dict}\nPayload: {payload_dict_or_generator}\n")
        #
        response = session.post(url_str, headers=headers_dict, json=payload_dict_or_generator, auth=auth_str_tuple)
    else:
        if debug_bool:
            print(f"POST Request\n-\nURL: {url_str}\nHeaders: {headers_dict}\nPayload: (generator)\n")
        #
        response = session.post(url_str, headers=headers_dict, data=payload_dict_or_generator, auth=auth_str_tuple)
    #
    if debug_bool:
        print(f"POST Response\n-\n{response.text}\n")
    #
    if is_json(response.text):
        response_dict = response.json()
        #
        if isinstance(response_dict, dict):
            if "error_code" in response_dict and response_dict["error_code"] > 400:
                raise Exception(response_dict["message"])
            #
        if response.ok:
            return response_dict
        else:
            raise Exception(response_dict)
    else:
        response_text_list = "[" + response.text[:-2].replace("\r\n", ",") + "]"
        response_dict_list = json.loads(response_text_list)
        #
        for response_dict in response_dict_list:
            if isinstance(response_dict, dict):
                if "error_code" in response_dict and response_dict["error_code"] > 400:
                    raise Exception(response_dict["message"])
        #
        if response.ok:
            return response_dict_list
        else:
            raise Exception(response_dict_list)


def get_auth_str_tuple(basic_auth_user_info):
    if basic_auth_user_info is None:
        auth_str_tuple = None
    else:
        auth_str_tuple = tuple(basic_auth_user_info.split(":"))
    #
    return auth_str_tuple


def is_json(str):
    try:
        json.loads(str)
    except ValueError as e:
        return False
    return True


def is_pattern(str):
    return "*" in str or "?" in str or ("[" in str and "]" in str) or ("[!" in str and "]" in str)


def is_base64_encoded(str_or_bytes_or_dict):
    try:
        if isinstance(str_or_bytes_or_dict, bytes):
            decoded_bytes = base64.b64decode(str_or_bytes_or_dict)
        elif isinstance(str_or_bytes_or_dict, str):
            decoded_bytes = base64.b64decode(bytes(str_or_bytes_or_dict, encoding="utf-8"))
        elif isinstance(str_or_bytes_or_dict, dict):
            decoded_bytes = base64.b64decode(bytes(json.dumps(str_or_bytes_or_dict, default=str), encoding="utf-8"))
        else:
            return False
        encoded_bytes = base64.b64encode(decoded_bytes)
        return str_or_bytes_or_dict == encoded_bytes
    except (binascii.Error, UnicodeDecodeError):
        return False


def base64_encode(str_or_bytes_or_dict):
    if isinstance(str_or_bytes_or_dict, bytes):
        encoded_bytes = base64.b64encode(str_or_bytes_or_dict)
    elif isinstance(str_or_bytes_or_dict, str):
        encoded_bytes = base64.b64encode(bytes(str_or_bytes_or_dict, encoding="utf-8"))
    elif isinstance(str_or_bytes_or_dict, dict):
        encoded_bytes = base64.b64encode(bytes(json.dumps(str_or_bytes_or_dict, default=str), encoding="utf-8"))
    return encoded_bytes


def base64_decode(base64_str):
    return base64.b64decode(bytes(base64_str, encoding="utf-8"))


def to_bytes(data):
    if isinstance(data, bytes):
        data_bytes = data
    elif isinstance(data, str):
        data_bytes = data.encode("utf-8")
    elif isinstance(data, dict):
        data_bytes = json.dumps(data, default=str).encode("utf-8")
    elif data is None:
        data_bytes = None
    else:
        data_str = str(data)
        data_bytes = data_str.encode("utf-8")
    #
    return data_bytes


def bytes_or_str_to_bytes(bytes_or_str):
    if isinstance(bytes_or_str, bytes):
        return_bytes = bytes_or_str
    elif isinstance(bytes_or_str, str):
        return_bytes = bytes(bytes_or_str, encoding="utf-8")
    #
    return return_bytes


def bytes_to_str(bytes):
    if bytes is None:
        str = None
    else:
        str = bytes.decode("utf-8")
    #
    return str


def bytes_to_dict(bytes):
    if bytes is None:
        dict = None
    else:
        dict = json.loads(bytes.decode("utf-8"))
    #
    return dict


def str_to_bytes(str):
    if str is None:
        bytes = None
    else:
        bytes = str.encode("utf-8")
    #
    return bytes


def pattern_match(input_str_list, pattern_str_or_str_list):
    if pattern_str_or_str_list is not None:
        if isinstance(pattern_str_or_str_list, str):
            pattern_str_or_str_list = [pattern_str_or_str_list]
        output_topic_str_list = [input_str for input_str in input_str_list if any(fnmatch(input_str, pattern_str) for pattern_str in pattern_str_or_str_list)]
    else:
        output_topic_str_list = input_str_list
    #
    output_topic_str_list.sort()
    #
    return output_topic_str_list


def explode_normalize(df):
    def explode(df, col_str):
        df = df.explode(col_str)
        #
        if isinstance(df.iloc[0][col_str], list):
            df = explode(df, col_str)
        elif isinstance(df.iloc[0][col_str], object):
            df_child = pd.json_normalize(df[col_str])
            df_child.columns = [f'{col_str}.{child_col_str}' for child_col_str in df_child.columns]
            df = pd.concat([df.loc[:, ~df.columns.isin([col_str])].reset_index(drop=True), df_child], axis=1)
        #
        return df
    #
    col_str_list = [col_str for col_str in df.columns if isinstance(df.iloc[0][col_str], list)]
    if len(col_str_list) < 1:
        return df
    #
    df = reduce(explode, col_str_list, df)
    #
    return df


def zip2(list1, list2):
    i = 0
    j = 0
    zipped_list = []
 
    while True:
        if len(list1) >= len(list2):
            if i < len(list1):
                zipped_list.append((list1[i], list2[j]))
                i += 1
                j += 1
            else:
                break
            if j >= len(list2):
                j = 0
        else:
            if j < len(list2):
                zipped_list.append((list1[i], list2[j]))
                i += 1
                j += 1
            else:
                break
            if i >= len(list1):
                i = 0
    
    return zipped_list


def s_id(payload_bytes):
    if payload_bytes is not None and len(payload_bytes) >= 5:
        id_int = int.from_bytes(payload_bytes[1:5], "big")
    else:
        id_int = -1
    #
    return id_int


def hash_dict(d):
    return hash(json.dumps(d, sort_keys=True))


def split_bytes(bytes, chunk_size_bytes_int):
    bytes_list = [bytes[i:i + chunk_size_bytes_int] for i in range(0, len(bytes), chunk_size_bytes_int)]
    #
    return bytes_list


def get_value(any, key_str_list):
    return reduce(lambda d, key_str: d.get(key_str, {}) if isinstance(d, dict) else None, key_str_list, any)


def set_value(d, key_str_list, any):
    for key in key_str_list[:-1]:
        if key not in d or not isinstance(d[key], dict):
            d[key] = {}
        d = d[key]
    d[key_str_list[-1]] = any


# def get_value_jsonpath(value_dict, jsonpath_str):
#     child = jsonpath_ng.parse(jsonpath_str)
#     datumInContextList = child.find(value_dict)
#     if len(datumInContextList) == 1:
#         return datumInContextList[0].value
#     elif len(datumInContextList) == 0:
#         raise Exception(f"Could not find a value with JSONPath expression {jsonpath_str} in dictionary {value_dict}")
#     else:
#         raise Exception(f"Could not unambiguously find a value with JSONPath expression {jsonpath_str} in dictionary {value_dict} (found: {[datumInContext.value for datumInContext in datumInContextList]})")

# def set_value_jsonpath(value_dict, jsonpath_str, any):
#     child = jsonpath_ng.parse(jsonpath_str)
#     child.update_or_create(value_dict, any)


# Partitioners

def default_partitioner(message_dict, counter_int, partitions_int, projection_function=lambda x: x["key"]):
    partition_int = message_dict["partition"]
    if partition_int == RD_KAFKA_PARTITION_UA:
        bytes = projection_function(message_dict)
        #
        if bytes is None:
            partition_int = counter_int
            if counter_int == partitions_int - 1:
                counter_int = 0
            else:
                counter_int += 1
        else:
            partition_int = hash(str(bytes)) % partitions_int
    #
    return partition_int

# Chunking

def message_dict_chunk_key_to_key(message_dict):
    chunk_key_bytes = message_dict["key"]
    key_bytes = chunk_key_to_key(chunk_key_bytes)
    return key_bytes

def chunk_key_to_key(chunk_key_bytes):
    if chunk_key_bytes == None:
        key_bytes = chunk_key_bytes
    else:
        key_bytes = chunk_key_bytes[:-7]
    #
    return key_bytes


def key_to_chunk_key(key_bytes, chunk_int):
    if key_bytes == None:
        chunk_key_bytes = key_bytes
    else:
        chunk_key_bytes = key_bytes + bytes(f"_{chunk_int:06}", "UTF-8")
    #
    return chunk_key_bytes
