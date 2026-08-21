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
import zstandard as zstd

from tqdm.auto import tqdm

#

zstdCompressor = zstd.ZstdCompressor(level=3)
zstdDecompressor = zstd.ZstdDecompressor()

# Constants

RD_KAFKA_PARTITION_UA = -1

#

def get_millis():
    """Current time in milliseconds since epoch."""
    return int(time.time()*1000)


def to_millis(timestamp_str):
    """Parse an ISO-8601 timestamp string into milliseconds since epoch."""
    return int(dateutil.parser.isoparse(timestamp_str).timestamp()*1000)


def from_millis(millis_int):
    """Format milliseconds since epoch as an ISO-ish timestamp string."""
    return datetime.datetime.fromtimestamp(millis_int/1000.0).isoformat(sep=" ")


def is_interactive():
    """True if running in an interactive Python shell."""
    return hasattr(sys, 'ps1')


def pretty(dict):
    """Pretty-print a dict as indented JSON (str)."""
    return json.dumps(dict, indent=2, default=str)


def ppretty(dict):
    """Print pretty(dict) directly to stdout."""
    print(pretty(dict))

#

def create_session(retries_int):
    """Build a requests.Session with retry/backoff for transient HTTP errors.

    Args:
        retries_int: max retry attempts on 500/502/503/504"""
    # logging.basicConfig(level=logging.DEBUG)
    session = requests.Session()
    retry = Retry(total=retries_int, backoff_factor=2, status_forcelist=[500, 502, 503, 504], allowed_methods=None)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    #
    return session


def get(url_str, headers_dict=None, payload_dict=None, auth_str_tuple=None, retries_int=0, debug_bool=False):
    """HTTP GET with retries, optional JSON payload, and error-code handling.

    Args:
        url_str: request URL
        headers_dict: HTTP headers
        payload_dict: optional JSON body (sent as query via json=)
        auth_str_tuple: optional (user, password) basic auth
        retries_int: max retry attempts
        debug_bool: if True, print request/response"""
    session = create_session(retries_int)
    #
    if payload_dict is None:
        if debug_bool:
            tqdm.write(f"GET Request\n-\nURL: {url_str}\nHeaders: {headers_dict}\n")
        #
        response = session.get(url_str, headers=headers_dict, auth=auth_str_tuple)
    else:
        if debug_bool:
            tqdm.write(f"GET Request\n-\nURL: {url_str}\nHeaders: {headers_dict}\nPayload: {payload_dict}")
        #
        response = session.get(url_str, headers=headers_dict, json=payload_dict, auth=auth_str_tuple)
    #
    if debug_bool:
        tqdm.write(f"GET Response\n-\n{response.text}\n")
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
    """HTTP DELETE with retries and error-code handling.

    Args:
        url_str: request URL
        headers_dict: HTTP headers
        auth_str_tuple: optional (user, password) basic auth
        retries_int: max retry attempts
        debug_bool: if True, print request/response"""
    session = create_session(retries_int)
    #
    if debug_bool:
        tqdm.write(f"DELETE Request\n-\nURL: {url_str}\nHeaders: {headers_dict}\n")
    #
    response = session.delete(url_str, headers=headers_dict, auth=auth_str_tuple)
    #
    if debug_bool:
        tqdm.write(f"DELETE Response\n-\n{response.text}\n")
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
    """HTTP POST with retries; accepts a JSON dict or a raw payload generator.

    Args:
        url_str: request URL
        headers_dict: HTTP headers
        payload_dict_or_generator: dict (sent as JSON) or a generator/bytes body
        auth_str_tuple: optional (user, password) basic auth
        retries_int: max retry attempts
        debug_bool: if True, print request/response"""
    session = create_session(retries_int)
    #
    if isinstance(payload_dict_or_generator, dict):
        if debug_bool:
            tqdm.write(f"POST Request\n-\nURL: {url_str}\nHeaders: {headers_dict}\nPayload: {payload_dict_or_generator}\n")
        #
        response = session.post(url_str, headers=headers_dict, json=payload_dict_or_generator, auth=auth_str_tuple)
    else:
        if debug_bool:
            tqdm.write(f"POST Request\n-\nURL: {url_str}\nHeaders: {headers_dict}\nPayload: (generator)\n")
        #
        response = session.post(url_str, headers=headers_dict, data=payload_dict_or_generator, auth=auth_str_tuple)
    #
    if debug_bool:
        tqdm.write(f"POST Response\n-\n{response.text}\n")
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
    """Split a "user:password" string into an (user, password) tuple.

    Args:
        basic_auth_user_info: "user:password" string, or None"""
    if basic_auth_user_info is None:
        auth_str_tuple = None
    else:
        auth_str_tuple = tuple(basic_auth_user_info.split(":"))
    #
    return auth_str_tuple


def is_json(str):
    """True if str parses as JSON."""
    try:
        json.loads(str)
    except ValueError as e:
        return False
    return True


def is_pattern(str):
    """True if str looks like a glob pattern (contains *, ?, or [...])."""
    return "*" in str or "?" in str or ("[" in str and "]" in str) or ("[!" in str and "]" in str)


def is_base64_encoded(str_or_bytes_or_dict):
    """True if str_or_bytes_or_dict round-trips through base64 decode/encode unchanged."""
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
    """Base64-encode bytes, str, or dict (JSON-serialized first)."""
    if isinstance(str_or_bytes_or_dict, bytes):
        encoded_bytes = base64.b64encode(str_or_bytes_or_dict)
    elif isinstance(str_or_bytes_or_dict, str):
        encoded_bytes = base64.b64encode(bytes(str_or_bytes_or_dict, encoding="utf-8"))
    elif isinstance(str_or_bytes_or_dict, dict):
        encoded_bytes = base64.b64encode(bytes(json.dumps(str_or_bytes_or_dict, default=str), encoding="utf-8"))
    return encoded_bytes


def base64_decode(base64_str):
    """Base64-decode a base64-encoded str into bytes."""
    return base64.b64decode(bytes(base64_str, encoding="utf-8"))


def to_bytes(data):
    """Convert bytes, str, dict (JSON), or any other value into UTF-8 bytes."""
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
    """Return bytes as-is, or UTF-8-encode a str."""
    if isinstance(bytes_or_str, bytes):
        return_bytes = bytes_or_str
    elif isinstance(bytes_or_str, str):
        return_bytes = bytes(bytes_or_str, encoding="utf-8")
    #
    return return_bytes


def bytes_to_str(bytes):
    """UTF-8-decode bytes into str (None stays None)."""
    if bytes is None:
        str = None
    else:
        str = bytes.decode("utf-8")
    #
    return str


def bytes_to_dict(bytes):
    """UTF-8-decode and JSON-parse bytes into a dict (None stays None)."""
    if bytes is None:
        dict = None
    else:
        dict = json.loads(bytes.decode("utf-8"))
    #
    return dict


def str_to_bytes(str):
    """UTF-8-encode a str into bytes (None stays None)."""
    if str is None:
        bytes = None
    else:
        bytes = str.encode("utf-8")
    #
    return bytes


def pattern_match(input_str_list, pattern_str_or_str_list):
    """Filter and sort input_str_list by one or more glob patterns.

    Args:
        input_str_list: candidate strings
        pattern_str_or_str_list: glob pattern, list of patterns, or None (matches all)"""
    if pattern_str_or_str_list is not None:
        if isinstance(pattern_str_or_str_list, str):
            pattern_str_or_str_list = [pattern_str_or_str_list]
        output_str_list = [input_str for input_str in input_str_list if any(fnmatch(input_str, pattern_str) for pattern_str in pattern_str_or_str_list)]
    else:
        output_str_list = input_str_list
    #
    output_str_list.sort()
    #
    return output_str_list


def explode_normalize(df):
    """Recursively explode list-valued columns and flatten nested dict columns of a pandas DataFrame."""
    def explode(df, col_str):
        df = df.explode(col_str)
        #
        if isinstance(df.iloc[0][col_str], list):
            df = explode(df, col_str)
        elif isinstance(df.iloc[0][col_str], object):
            df_child = pd.json_normalize(df[col_str])
            df_child.columns = [f'{col_str}.{child_col_str}' for child_col_str in df_child.columns]
            df_cleaned = df.drop(columns=[col_str]).reset_index(drop=True)
            df = pd.concat([df_cleaned, df_child.reset_index(drop=True)], axis=1)
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


def s_id(payload_bytes):
    """Extract the 4-byte Schema Registry schema id from a Confluent-framed payload (-1 if absent)."""
    if payload_bytes is not None and len(payload_bytes) >= 5:
        id_int = int.from_bytes(payload_bytes[1:5], "big")
    else:
        id_int = -1
    #
    return id_int


def hash_dict(d):
    """Stable hash of a dict, based on its sorted-key JSON representation."""
    return hash(json.dumps(d, sort_keys=True))


def split_bytes(bytes, chunk_size_bytes_int):
    """Split bytes into fixed-size chunks.

    Args:
        bytes: data to split
        chunk_size_bytes_int: max size of each chunk"""
    bytes_list = [bytes[i:i + chunk_size_bytes_int] for i in range(0, len(bytes), chunk_size_bytes_int)]
    #
    return bytes_list


def get_value(any, key_str_list):
    """Look up a nested value by a path of dict keys.

    Args:
        any: dict (or nested dicts) to look up into
        key_str_list: path of keys to follow"""
    return reduce(lambda d, key_str: d.get(key_str, {}) if isinstance(d, dict) else None, key_str_list, any)


def set_value(d, key_str_list, any):
    """Set a nested value by a path of dict keys, creating intermediate dicts as needed.

    Args:
        d: dict to mutate
        key_str_list: path of keys to follow
        any: value to set at the final key"""
    for key in key_str_list[:-1]:
        if key not in d or not isinstance(d[key], dict):
            d[key] = {}
        d = d[key]
    d[key_str_list[-1]] = any

# Partitioners

def default_partitioner(m, counter_int, partitions_int, projection_fun=lambda x: x["key"]):
    """Resolve which partition a message goes to: explicit partition, else hash or round-robin.

    Args:
        m: message dict; its "partition" is used if not RD_KAFKA_PARTITION_UA
        counter_int: current round-robin counter, used when projection_fun returns None
        partitions_int: number of partitions to choose among
        projection_fun: m -> bytes used for hash partitioning"""
    partition_int = m["partition"]
    if partition_int == RD_KAFKA_PARTITION_UA:
        bytes = projection_fun(m)
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

def m_chunk_key_to_key(m):
    """Recover the original message key from a chunked message's key."""
    chunk_key_bytes = m["key"]
    key_bytes = chunk_key_to_key(chunk_key_bytes)
    return key_bytes

def chunk_key_to_key(chunk_key_bytes):
    """Strip the trailing chunk suffix from a chunk key, recovering the original key bytes."""
    if chunk_key_bytes == None:
        key_bytes = chunk_key_bytes
    else:
        key_bytes = chunk_key_bytes[:-7]
    #
    return key_bytes


def key_to_chunk_key(key_bytes, chunk_int):
    """Append a chunk-index suffix to a key.

    Args:
        key_bytes: original key bytes
        chunk_int: chunk index to encode into the suffix"""
    if key_bytes == None:
        chunk_key_bytes = key_bytes
    else:
        chunk_key_bytes = key_bytes + bytes(f"_{chunk_int:06}", "UTF-8")
    #
    return chunk_key_bytes


def is_internal(resource_str):
    """True if resource_str is an internal (underscore-prefixed) resource name."""
    return resource_str.startswith("_")


def copy_kwargs(name_str, **kwargs):
    """Copy kwargs, renaming any "{name_str}_xxx" keys to their plain "xxx" form.

    Args:
        name_str: prefix identifying which prefixed keys to rename, e.g. "source"
        **kwargs: original kwargs, e.g. containing "source_group", "source_type", etc."""
    copied_kwargs = kwargs.copy()
    #
    if f"{name_str}_group" in kwargs:
        copied_kwargs["group"] = kwargs[f"{name_str}_group"]
    if f"{name_str}_offsets" in kwargs:
        copied_kwargs["offsets"] = kwargs[f"{name_str}_offsets"]
    if f"{name_str}_key_type" in kwargs:
        copied_kwargs["key_type"] = kwargs[f"{name_str}_key_type"]
    if f"{name_str}_value_type" in kwargs:
        copied_kwargs["value_type"] = kwargs[f"{name_str}_value_type"]
    if f"{name_str}_type" in kwargs:
        copied_kwargs["type"] = kwargs[f"{name_str}_type"]
    if f"{name_str}_key_schema" in kwargs:
        copied_kwargs["key_schema"] = kwargs[f"{name_str}_key_schema"]
    if f"{name_str}_value_schema" in kwargs:
        copied_kwargs["value_schema"] = kwargs[f"{name_str}_value_schema"]
    if f"{name_str}_key_schema_id" in kwargs:
        copied_kwargs["key_schema_id"] = kwargs[f"{name_str}_key_schema_id"]
    if f"{name_str}_value_schema_id" in kwargs:
        copied_kwargs["value_schema_id"] = kwargs[f"{name_str}_value_schema_id"]
    #
    return copied_kwargs


def compress(uncompressed_bytes):
    """Zstandard-compress bytes."""
    return zstdCompressor.compress(uncompressed_bytes)


def decompress(compressed_bytes):
    """Zstandard-decompress bytes."""
    return zstdDecompressor.decompress(compressed_bytes)
