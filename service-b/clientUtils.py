import pyarrow as pa
import pyarrow.flight as flight
import json


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