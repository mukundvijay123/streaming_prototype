import multiprocessing
from time import sleep
import pyarrow.flight as flight
import multiprocessing.shared_memory
from Client import subscribe_test
from FlightServer import FlightServer
from event_consumer import continuous_consumer,consumer
from datetime import datetime

BUFFER_SIZE = 10000
EVENT_SIZE = 4096

# Create shared memory and synchronization primitives
shared_memory = multiprocessing.shared_memory.SharedMemory(create=True, size=BUFFER_SIZE * EVENT_SIZE)
lock = multiprocessing.Lock()
write_index = multiprocessing.Value('i', 0)
read_index = multiprocessing.Value('i', 0)

def startFlightServer(shared_memory_name, lock, write_index):
    server = FlightServer(shared_memory_name, lock, write_index, location='grpc://127.0.0.1:8816')
    print(f"[{datetime.now().isoformat()}] [Main] FLIGHT SERVER STARTING | Port 8816")
    server.serve()


if __name__ == "__main__":
    FlightServerAddress = 'grpc://127.0.0.1:8816'
    RemoteAddress = 'grpc://127.0.0.1:8815'
    
    print(f"[{datetime.now().isoformat()}] [Main] INIT | Starting system...")
    
    # Start FlightServer
    server_process = multiprocessing.Process(
        target=startFlightServer, 
        args=(shared_memory.name, lock, write_index),
        daemon=True
    )
    server_process.start()
    sleep(2)

    # Start event consumer
    consumer_process = multiprocessing.Process(
        target=continuous_consumer,
        args=(shared_memory.name, lock, write_index, read_index,BUFFER_SIZE,EVENT_SIZE),
        daemon=True
    )
    consumer_process.start()
    print(f"[{datetime.now().isoformat()}] [Main] CONSUMER STARTED | PID: {consumer_process.pid}")

    # Initiate data transfer
    print(f"[{datetime.now().isoformat()}] [Main] SUBSCRIBING | Connecting to {RemoteAddress}")
    subscribe_test(RemoteAddress, FlightServerAddress)

    try:
        print(f"[{datetime.now().isoformat()}] [Main] RUNNING | Press Ctrl+C to stop")
        while True:
            sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{datetime.now().isoformat()}] [Main] SHUTDOWN STARTED")
        server_process.terminate()
        consumer_process.terminate()
        shared_memory.close()
        shared_memory.unlink()
        print(f"[{datetime.now().isoformat()}] [Main] SHUTDOWN COMPLETE | Resources released")