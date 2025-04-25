import json 
import re
def extract_subscription(action):
    try:
        data = json.loads(action.body.to_pybytes().decode("utf-8"))
        print(data)
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
