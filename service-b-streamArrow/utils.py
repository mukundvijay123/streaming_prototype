import json 
import re
def extract_subscription(action):
    try:
        data = json.loads(action.body.to_pybytes().decode("utf-8"))
        if "address"  not in data or "topic" not in data:
            raise ValueError("Action body does not contain valid subscription fields")
        return (data["address"],data["topic"])
    except Exception as e:
        raise ValueError(f"Failed to extract address: {e}")




def is_valid_grpc_address(address: str) -> bool:
    # Allow optional grpc:// or grpcs:// prefix
    prefix = r'^(grpcs?://)?'

    # IPv4/hostname + port (e.g., grpc://localhost:50051)
    ipv4_hostname_port = prefix + r'([a-zA-Z0-9\.\-]+):(\d{1,5})$'
    # IPv6 in brackets + port (e.g., grpc://[::1]:50051)
    ipv6_port = prefix + r'\[([0-9a-fA-F:]+)\]:(\d{1,5})$'

    match = re.match(ipv4_hostname_port, address) or re.match(ipv6_port, address)
    if not match:
        return False

    try:
        port = int(match.group(3))  # Port is always the third group due to optional prefix
        return 1 <= port <= 65535
    except (IndexError, ValueError):
        return False
    



    
def find_topics(json_substrait_plans, tables=None, errors=None):
    """
    Perform a DFS traversal of a nested dictionary/list structure to find
    all 'named_table' keys and their values, with error handling.

    Args:
        json_substrait_plans: The nested dict/list to traverse.
        tables: Internal list to collect found tables.
        errors: Internal list to collect error messages.

    Returns:
        (topics, errors):
          topics: List of unique named_table values found.
          errors: List of error strings encountered during traversal.
    """
    if tables is None:
        tables = []
    if errors is None:
        errors = []

    try:
        if isinstance(json_substrait_plans, dict):
            # Attempt to extract named_table if present
            if "named_table" in json_substrait_plans:
                try:
                    names = json_substrait_plans["named_table"]["names"]
                    if isinstance(names, str):
                        tables.append(names)
                    elif isinstance(names, list):
                        tables.extend(names)
                    else:
                        raise TypeError(f"'names' is not str or list: {names!r}")
                except Exception as e:
                    errors.append(f"Error extracting named_table.names: {e}")

            # Recurse into all values
            for value in json_substrait_plans.values():
                find_topics(value, tables, errors)

        elif isinstance(json_substrait_plans, list):
            for item in json_substrait_plans:
                find_topics(item, tables, errors)

        # Other types (str, int, etc.) are ignored
    except Exception as e:
        # Catch unexpected errors at this node
        print(e)

    # Return unique topics and any errors
    return list(set(tables))