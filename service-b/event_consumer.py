import multiprocessing
from datetime import datetime
from time import sleep

def consumer(shared_mem, lock, write_index, read_index,BUFFER_SIZE,EVENT_SIZE):
    current_time = datetime.now().isoformat()
    try:
        with lock:
            if read_index.value!=write_index.value:
                start_pos = read_index.value * EVENT_SIZE
                event_bytes = shared_mem.buf[start_pos:start_pos + EVENT_SIZE]
                event_str = event_bytes.tobytes().decode('utf-8').strip('\x00')

                
                print(f"[{current_time}] [Consumer] READ POSITION | {read_index.value}")
                print(f"[{current_time}] [Consumer] CONTENT | {event_str[:200]}...")

                # Update read index
                old_read_index = read_index.value
                read_index.value = (read_index.value + 1) % BUFFER_SIZE
                print(f"[{current_time}] [Consumer] INDEX UPDATE | {old_read_index}→{read_index.value}")
                print(f"[{current_time}] [Consumer] EVENT EXTRACTED ")
                print(f"[{current_time}] [Consumer] DATA SENT TO WEBSOCKET SERVER | {event_str[:200]}...")
                return event_str
            else:
                print(f"[{current_time}] [Consumer] NO NEW DATA TO CONSUME")
                return None
    except Exception as e:
        print("error has occured here")
        print(e)
    finally:
       pass



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