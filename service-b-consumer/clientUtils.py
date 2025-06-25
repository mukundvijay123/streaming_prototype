import pyarrow as pa
import pyarrow.flight as flight
import json
import httpx
import asyncio

def subscribe(topic,RemoteAddress, FlightServerAddress):
    try:
        # Establish connection
        flight_client = flight.connect(RemoteAddress)
        
        # Prepare payload
        payload = {
            "address": FlightServerAddress,
            "topic":topic
        }
        payload_bytes = json.dumps(payload).encode("utf-8")
        
        # Create subscription action
        action = flight.Action("subscribe", payload_bytes)
        
        # Perform action and handle responses
        try:
            results = list(flight_client.do_action(action))
            
            if not results:
                print("No responses received from subscription.")
                return
            
            for response in results:
                try:
                    response_str = response.body.to_pybytes().decode("utf-8")
                    print("Server response:", response_str)
                except Exception as decode_error:
                    print(f"Error decoding response: {decode_error}")
        
        except flight.FlightError as action_error:
            print(f"Flight action error during subscription: {action_error}")
    
    except Exception as conn_error:
        print(f"Error connecting to Flight server: {conn_error}")



def unsubscribe(topic ,RemoteAddress, FlightServerAddress):
    try:
        # Establish connection
        flight_client = flight.connect(RemoteAddress)
        
        # Prepare payload
        payload = {
            "address": FlightServerAddress,
            "topic":topic
        }
        payload_bytes = json.dumps(payload).encode("utf-8")
        
        # Create unsubscription action
        action = flight.Action("unsubscribe", payload_bytes)
        
        # Perform action and handle responses
        try:
            results = list(flight_client.do_action(action))
            
            if not results:
                print("No responses received from unsubscription.")
                return
            
            for response in results:
                try:
                    response_str = response.body.to_pybytes().decode("utf-8")
                    print("Server response:", response_str)
                except Exception as decode_error:
                    print(f"Error decoding response: {decode_error}")
        
        except flight.FlightError as action_error:
            print(f"Flight action error during unsubscription: {action_error}")
    
    except Exception as conn_error:
        print(f"Error connecting to Flight server: {conn_error}")


async def check_access_async(base_url, token, topic, action) -> bool:
    url = f"{base_url}/authorize"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    params = {
        "topic": topic,
        "action": action
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, params=params)
            return response.status_code == 200
        except Exception as e:
            print(f"Error checking access: {e}")
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
        topics: List of unique named_table values found.
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

    # Return unique topics
    return list(set(tables))