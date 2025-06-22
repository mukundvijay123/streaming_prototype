import json 
import re
import requests
import httpx
import asyncio
from metadata import systemMetadata
from queueMap import QueueMap
def extract_subscription(action):
    try:
        data = json.loads(action.body.to_pybytes().decode("utf-8"))
        if "address"  not in data or "topic" not in data:
            raise ValueError("Action body does not contain valid subscription fields")
        return (data["address"],data["topic"],data['auth']['token'])
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


def add_topic_to_system(topic_name:str,schema:str,javaServerAddr:str,system_metadata:systemMetadata,queue_map:QueueMap):
    """
    Sends a request to the Java server to add a topic and schema.
    If successful, adds the topic to system metadata and queue map.
    """
    url = f"{javaServerAddr}/create"
    payload = {
        "topic": topic_name,
        "createTableStatement": schema
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 201:  # Success
            system_metadata.addTopic(topic_name)
            queue_map.add_topic(topic_name)
            print(f"Topic '{topic_name}' successfully added to system metadata and queue map.")
        else:
            print(f"Failed to add topic '{topic_name}': {response.json().get('error', 'Unknown error')}")
    except Exception as e:
        print(f"Error while communicating with Java server: {e}")


def check_access(base_url, token, topic, action) -> bool:
    url = f"{base_url}/authorize"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    params = {
        "topic": topic,
        "action": action
    }

    try:
        with httpx.Client() as client:
            response = client.get(url, headers=headers, params=params)
            return response.status_code == 200
    except Exception as e:
        print(f"Error checking access: {e}")
        return False