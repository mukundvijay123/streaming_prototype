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





def continuous_consumer(shared_memory_name, lock, write_index, read_index,BUFFER_SIZE,EVENT_SIZE):
    shared_mem = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
    event_counter = 0
    
    print(f"[{datetime.now().isoformat()}] [Consumer] INIT | Ready to consume events")
    
    while True:
        current_time = datetime.now().isoformat()
        with lock:
            if read_index.value != write_index.value:
                event_counter += 1
                start_pos = read_index.value * EVENT_SIZE
                event_bytes = shared_mem.buf[start_pos:start_pos + EVENT_SIZE]
                event_str = event_bytes.tobytes().decode('utf-8').strip('\x00')
                print(f"[{current_time}] [Continuous_consumer] RAW DATA | {event_str[:100]}...")
                
                # Print detailed consumption info
                print(f"\n[{current_time}] [Continuous_Consumer] NEW EVENT #{event_counter}")
                print(f"[{current_time}] [Continuous_Consumer] READ POSITION | {read_index.value}")
                print(f"[{current_time}] [Continuous_Consumer] CONTENT | {event_str[:200]}...")
                
                # Update read index
                old_read_index = read_index.value
                read_index.value = (read_index.value + 1) % BUFFER_SIZE
                print(f"[{current_time}] [Continuous_Consumer] INDEX UPDATE | {old_read_index}→{read_index.value}")
                
                # Buffer status
                buffer_usage = (write_index.value - read_index.value) % BUFFER_SIZE
                print(f"[{current_time}] [Continuous_Consumer] BUFFER STATUS | {buffer_usage}/{BUFFER_SIZE} slots used")
            else:
                print(f"[{current_time}] [Continuous_Consumer] WAITING | No new events")
            

            sleep(1)