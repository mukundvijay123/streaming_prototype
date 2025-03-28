import pyarrow.flight as flight
import multiprocessing.shared_memory
import numpy as np
import ctypes


BUFFER_SIZE = 10000  # Same as in service-b.py
EVENT_SIZE = 1024  # Same as in service-b.py

class FlightServer(flight.FlightServerBase):
    def __init__(self, shared_memory_name, lock, write_index, location="grpc://127.0.0.1:8815"):
        super(FlightServer, self).__init__(location)
        self.location = location
        self.shared_memory = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
        self.lock = lock
        self.write_index = write_index

        
     #   print(f"[FlightServer] Memory Address: {ctypes.addressof(self.shared_memory.buf)}")

    def do_put(self, context, descriptor, reader, writer):
        event_descriptor = descriptor.path[0].decode('utf-8')
        full_table = reader.read_all()
        event_dict = full_table.to_pydict()
        event_str = str(event_dict)

        with self.lock:
            if self.write_index.value < BUFFER_SIZE:
                start_pos = self.write_index.value * EVENT_SIZE
                event_bytes = event_str.encode('utf-8')[:EVENT_SIZE]  # Truncate if too large
                self.shared_memory.buf[start_pos:start_pos + len(event_bytes)] = event_bytes
                self.write_index.value += 1  # Increment index
                print(f"Added event to shared memory: {event_str}")
            else:
                print("Shared memory buffer full, event dropped.")
