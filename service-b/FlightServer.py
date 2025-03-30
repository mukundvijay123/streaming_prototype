import pyarrow.flight as flight
import multiprocessing.shared_memory
import ctypes
from datetime import datetime

BUFFER_SIZE = 10000
EVENT_SIZE = 4096

class FlightServer(flight.FlightServerBase):
    def __init__(self, shared_memory_name, lock, write_index, location="grpc://127.0.0.1:8815"):
        super(FlightServer, self).__init__(location)
        self.location = location
        self.shared_memory = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
        self.lock = lock
        self.write_index = write_index
        self.event_counter = 0
        print(f"[{datetime.now().isoformat()}] [FlightServer] INIT | SharedMem: {shared_memory_name} | WriteIdx: {write_index.value}")

    def do_put(self, context, descriptor, reader, writer):
        self.event_counter += 1
        current_time = datetime.now().isoformat()
        print(f"\n[{current_time}] [FlightServer] START PUT | Event #{self.event_counter}")
        
        try:
            event_descriptor = descriptor.path[0].decode('utf-8')
            print(f"[{current_time}] [FlightServer] DESCRIPTOR | {event_descriptor}")
            
            full_table = reader.read_all()
            event_dict = full_table.to_pydict()
            event_str = str(event_dict)
            print(f"[{current_time}] [FlightServer] RAW DATA | {event_str[:100]}...")

            with self.lock:
                current_write_pos = self.write_index.value
                start_pos = current_write_pos * EVENT_SIZE
                
                # Prepare and write data
                event_bytes = event_str.encode('utf-8')[:EVENT_SIZE]
                self.shared_memory.buf[start_pos:start_pos + len(event_bytes)] = event_bytes
                
                # Update index
                new_write_index = (current_write_pos + 1) % BUFFER_SIZE
                self.write_index.value = new_write_index
                
                print(f"[{current_time}] [FlightServer] WRITE COMPLETE | Pos: {current_write_pos}→{new_write_index}")
                print(f"[{current_time}] [FlightServer] CONTENT | {event_str[:50]}...")
                print(f"[{current_time}] [FlightServer] BUFFER STATUS | Used: {(new_write_index - current_write_pos) % BUFFER_SIZE}/{BUFFER_SIZE}")

        except Exception as e:
            print(f"[{current_time}] [FlightServer] ERROR | {str(e)}")
            raise
        finally:
            print(f"[{datetime.now().isoformat()}] [FlightServer] END PUT | Event #{self.event_counter}")

    def __del__(self):
        print(f"[{datetime.now().isoformat()}] [FlightServer] CLEANUP | Resources released")
        self.shared_memory.close()