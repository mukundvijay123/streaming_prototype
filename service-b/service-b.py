import multiprocessing
from time import sleep
import pyarrow as pa
import pyarrow.flight as flight
import multiprocessing.shared_memory
from datetime import datetime
from clientUtils import subscribe, unsubscribe
from FlightServer import FlightServer
from multiprocessing import Event
from websocketserver import start_websocket_server
from SharedMemoryResources import SharedMemoryResources

BUFFER_SIZE = 1000  # Max number of messages
HEADER_SIZE = 20  # 8 bytes for size, 8 bytes for offset
DATA_SECTION_SIZE = 1024*1024*100   # 100MB for data section

def startFlightServer(shared_memory_name, lock, write_index, read_index, data_section_start, 
                     write_data_idx, read_data_idx, event, event2, header_size, buffer_size):
    server = FlightServer(
        shared_memory_name, 
        lock, 
        write_index, 
        read_index,
        data_section_start,
        write_data_idx,
        read_data_idx,
        location='grpc://127.0.0.1:8816',
        event=event,
        event2=event2,
        header_size=header_size,
        buffer_size=buffer_size
    )
    print(f"[{datetime.now().isoformat()}] [Main] FLIGHT SERVER STARTING | Port 8816")
    server.serve()

if __name__ == "__main__":
    # Calculate shared memory layout
    headers_size = BUFFER_SIZE * HEADER_SIZE
    total_memory_size = headers_size + DATA_SECTION_SIZE
    
    # Create shared memory and synchronization primitives
    shm = multiprocessing.shared_memory.SharedMemory(create=True, size=total_memory_size)
    lock = multiprocessing.Lock()
    write_index = multiprocessing.Value('i', 0)
    read_index = multiprocessing.Value('i', 0)
    data_section_start = multiprocessing.Value('i', headers_size)
    write_data_idx = multiprocessing.Value('i', 0)  # Track write position in data section
    read_data_idx = multiprocessing.Value('i', 0)   # Track read position in data section
    event = Event()
    event2 = Event()
    FlightServerAddress = 'grpc://127.0.0.1:8816'
    RemoteAddress = 'grpc://127.0.0.1:8815'
    
    print(f"[{datetime.now().isoformat()}] [Main] INIT | Starting system...")
    print(f"[{datetime.now().isoformat()}] [Main] MEMORY | Headers: {headers_size/1024:.1f}KB, Data: {DATA_SECTION_SIZE/1024/1024:.1f}MB")
    
    # Start FlightServer
    server_process = multiprocessing.Process(
        target=startFlightServer, 
        args=(shm.name, lock, write_index, read_index, data_section_start, write_data_idx, read_data_idx, event, event2,HEADER_SIZE,BUFFER_SIZE),
        daemon=True
    )
    server_process.start()
    sleep(2)

    websocket_process = multiprocessing.Process(
        target=start_websocket_server,
        args=(shm.name, lock, write_index, read_index, data_section_start, write_data_idx, read_data_idx, event, event2,HEADER_SIZE,BUFFER_SIZE),
        daemon=True
    )
    websocket_process.start()
    print(f"[{datetime.now().isoformat()}] [Main] WEBSOCKET SERVER STARTED | PID: {websocket_process.pid}")

    # Initiate data transfer
    print(f"[{datetime.now().isoformat()}] [Main] SUBSCRIBING | Connecting to {RemoteAddress}")
    subscribe("ABC",RemoteAddress, FlightServerAddress)
    subscribe("LMN",RemoteAddress, FlightServerAddress)
    subscribe("XYZ",RemoteAddress, FlightServerAddress)
    try:
        print(f"[{datetime.now().isoformat()}] [Main] RUNNING | Monitoring data flow")
        while True:
            sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{datetime.now().isoformat()}] [Main] SHUTDOWN STARTED | Stopping data flow")
        server_process.terminate()
        websocket_process.terminate()
        shm.close()
        shm.unlink()
        print(f"[{datetime.now().isoformat()}] [Main] SHUTDOWN COMPLETE | Resources released")
        print(f"[{datetime.now().isoformat()}] [Main] WEBSOCKET SERVER TERMINATED")
