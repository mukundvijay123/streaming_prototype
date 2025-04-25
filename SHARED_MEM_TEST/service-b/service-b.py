import multiprocessing
from time import sleep
import pyarrow as pa
import pyarrow.flight as flight
import multiprocessing.shared_memory
from datetime import datetime
import logging
from clientUtils import subscribe, unsubscribe
from FlightServer import FlightServer
from multiprocessing import Event
from SharedMemoryResources import SharedMemoryResources
import uvicorn
import os

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("service_b")

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
    logger.info("FLIGHT SERVER STARTING | Port 8816")
    server.serve()

def start_substrait_service(shared_memory_name, lock, write_index, read_index,
                           data_section_start, write_data_idx, read_data_idx,
                           event, event2, header_size, buffer_size, host="0.0.0.0", port=8765):
    """
    Start the substrait service with access to shared memory
    """
    # Create SharedMemoryResources to access the shared memory
    logger.info(f"Creating SharedMemoryResources with name: {shared_memory_name}")
    try:
        shm_raw = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
        shm = SharedMemoryResources(
            shm_raw, lock, write_index, read_index,
            data_section_start, write_data_idx, read_data_idx,
            event, event2, header_size, buffer_size
        )
        
        # Setup the shared memory access for the FastAPI app
        # We need to make the shm instance available to the app
        # Import here to avoid circular imports
        from substrait_service import setup_shared_memory, app as substrait_app
        
        logger.info("Setting up shared memory for the substrait service")
        setup_shared_memory(shm)
        
        logger.info(f"Starting substrait service on {host}:{port}")
        uvicorn.run(substrait_app, host=host, port=port)
    except Exception as e:
        logger.error(f"Error starting substrait service: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    # Calculate shared memory layout
    headers_size = BUFFER_SIZE * HEADER_SIZE
    total_memory_size = headers_size + DATA_SECTION_SIZE
    
    logger.info(f"INIT | Starting system...")
    logger.info(f"MEMORY | Headers: {headers_size/1024:.1f}KB, Data: {DATA_SECTION_SIZE/1024/1024:.1f}MB")
    
    # Create shared memory and synchronization primitives
    try:
        shm = multiprocessing.shared_memory.SharedMemory(create=True, size=total_memory_size)
        logger.info(f"Shared memory created with name: {shm.name}")
        
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
        
        # Start FlightServer
        server_process = multiprocessing.Process(
            target=startFlightServer, 
            args=(shm.name, lock, write_index, read_index, data_section_start, write_data_idx, read_data_idx, event, event2, HEADER_SIZE, BUFFER_SIZE),
            daemon=True
        )
        server_process.start()
        logger.info(f"Flight server started with PID: {server_process.pid}")
        sleep(2)

        # Start Substrait Service
        substrait_process = multiprocessing.Process(
            target=start_substrait_service,
            args=(shm.name, lock, write_index, read_index, data_section_start, write_data_idx, read_data_idx, event, event2, HEADER_SIZE, BUFFER_SIZE),
            daemon=True
        )
        substrait_process.start()
        logger.info(f"SUBSTRAIT SERVICE STARTED | PID: {substrait_process.pid}")

        # Initiate data transfer
        logger.info(f"SUBSCRIBING | Connecting to {RemoteAddress}")
        subscribe("ABC", RemoteAddress, FlightServerAddress)
        subscribe("LMN", RemoteAddress, FlightServerAddress)
        subscribe("XYZ", RemoteAddress, FlightServerAddress)
        
        logger.info(f"RUNNING | Monitoring data flow")
        try:
            while True:
                sleep(1)
        except KeyboardInterrupt:
            logger.info(f"SHUTDOWN STARTED | Stopping data flow")
            server_process.terminate()
            substrait_process.terminate()
            shm.close()
            shm.unlink()
            logger.info(f"SHUTDOWN COMPLETE | Resources released")
            logger.info(f"SUBSTRAIT SERVICE TERMINATED")
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        import traceback
        logger.error(traceback.format_exc())