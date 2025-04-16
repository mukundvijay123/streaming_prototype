# simple_reader.py
import multiprocessing.shared_memory
import pyarrow as pa
import time
import json
from datetime import datetime
from SharedMemoryResources import SharedMemoryResources

def simple_reader_process(shared_memory_name, lock, write_index, read_index, 
                         data_section_start, write_data_idx, read_data_idx, 
                         event, event2):
    """
    Simple reader process that continuously reads events from shared memory
    and prints them without WebSocket functionality.
    """
    print(f"[{datetime.now().isoformat()}] [Reader] Starting reader process")
    
    # Connect to the shared memory segment
    shm = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
    
    # Create SharedMemoryResources object
    shared_memory = SharedMemoryResources(
        shm,
        lock,
        write_index,
        read_index,
        data_section_start,
        write_data_idx,
        read_data_idx,
        event,
        event2
    )
    
    try:
        while True:
            # Read data from shared memory
            event_data = shared_memory.read()
            #print(event.schema.metadata)
            
            # If we got data, print it
            if isinstance(event_data,pa.Table):
                print("Hellooooooo",(event_data.schema.metadata[b"topic"]))
                #print(f"[{datetime.now().isoformat()}] [Reader] Received event: {json.dumps(event_data, default=str)}")
                
            # Sleep for a short time before checking again
            
    except KeyboardInterrupt:
        print(f"[{datetime.now().isoformat()}] [Reader] Process terminated by user")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] [Reader] Error: {str(e)}")
    finally:
        # Clean up shared memory when done
        shm.close()
        print(f"[{datetime.now().isoformat()}] [Reader] Reader process shut down")

