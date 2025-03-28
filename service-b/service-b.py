import multiprocessing
import numpy as np
import pyarrow as pa
from FlightServer import FlightServer
from Client import subscribe_test
from time import sleep
import ctypes
# Constants
import pyarrow.flight as flight
import multiprocessing.shared_memory
import numpy as np

BUFFER_SIZE = 10000  # Same as in service-b.py
EVENT_SIZE = 1024  # Assume each event is max 1024 bytes

# Create shared memory and synchronization primitives
shared_memory = multiprocessing.shared_memory.SharedMemory(create=True, size=BUFFER_SIZE * EVENT_SIZE)
lock = multiprocessing.Lock()
write_index = multiprocessing.Value('i', 0)  # Shared integer for writing position

def startFlightServer(shared_memory_name, lock, write_index):
    FlightServerAddress = 'grpc://127.0.0.1:8816'
    server = FlightServer(shared_memory_name, lock, write_index, location=FlightServerAddress)
    print(f"Starting FlightServer at {FlightServerAddress}...")
    server.serve()


def event_consumer(shared_memory_name, lock, write_index):
    shared_mem = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
    memory_address = ctypes.c_void_p.from_buffer(shared_mem.buf).value

    while True:
        with lock:
            if write_index.value > 0:
                # Read latest event
                event_bytes = shared_mem.buf[(write_index.value - 1) * EVENT_SIZE: write_index.value * EVENT_SIZE]
                event_str = event_bytes.tobytes().decode('utf-8').strip('\x00')  # Remove null padding
                print("Received Event:", event_str)
                write_index.value -= 1  # Move index back to indicate consumption
        sleep(1)

if __name__ == "__main__":
    FlightServerAddress = 'grpc://127.0.0.1:8816'
    RemoteAddress = 'grpc://127.0.0.1:8815'
    
    # Start FlightServer
    server_process = multiprocessing.Process(target=startFlightServer, args=(shared_memory.name, lock, write_index), daemon=True)
    server_process.start()
    sleep(2)

    # Send request to Service A
    subscribe_test(RemoteAddress, FlightServerAddress)

    # Start event consumer
    consumer_process = multiprocessing.Process(target=event_consumer, args=(shared_memory.name, lock, write_index), daemon=True)
    consumer_process.start()
    print(f"[{consumer_process.pid}] Event Consumer Process Started")
    consumer_process.join()