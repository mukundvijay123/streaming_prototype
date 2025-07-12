import pyarrow as pa
import pyarrow.flight as flight
import json
import httpx
import asyncio

class clientCtx:
    def __init__(self,token:str,action:str="statelessRead"):
        self.token=token
        self.action=action


def subscribe(topic:str,RemoteAddress:str, FlightServerAddress:str,ctx:clientCtx):
    try:
        # Establish connection
        flight_client = flight.connect(RemoteAddress)
        
        # Prepare payload
        payload = {
            "address": FlightServerAddress,
            "topic":topic,
            "auth":{
                "token":ctx.token,
                "action":ctx.action
            }
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


#For rbac
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