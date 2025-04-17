import pyarrow as pa
import pyarrow.flight as flight
import multiprocessing.shared_memory
from datetime import datetime
from SharedMemoryResources import SharedMemoryResources
#0                   16 * 10K, 
#[HEADER1, HEADER2, ... 10000, DATA1, DATA2, ...]
# R, W                           R_D_I, W_D_I
#[SIZE -> 8 BYTES]
#[OFFSET -> 8 BYTES] 
class FlightServer(flight.FlightServerBase):
    def __init__(self, shared_memory_name, lock, write_index, read_index, data_section_start, 
                write_data_idx, read_data_idx, location, event, event2,header_size,buffer_size):
        super().__init__(location)
        self.shm_name = shared_memory_name
        self.event = event
        self.lock = lock
        self.write_index = write_index
        self.read_index = read_index
        self.data_section_start = data_section_start
        self.write_data_idx = write_data_idx
        self.read_data_idx = read_data_idx
        self.shm = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
        self.data_section_size = self.shm.size - self.data_section_start.value
        self.event2 = event2
        self.header_size = header_size
        self.buffer_size = buffer_size
    def do_put(self, context, descriptor, reader, flight_writer):
        try:
            table = reader.read_all()
            
            # Use the SharedMemoryResources class to write data
            shared_memory = SharedMemoryResources(
                multiprocessing.shared_memory.SharedMemory(name=self.shm_name),#THIS IS THR NAME
                self.lock,
                self.write_index,
                self.read_index,
                self.data_section_start,
                self.write_data_idx,
                self.read_data_idx,
                self.event,
                self.event2,
                self.header_size,
                self.buffer_size
            )
            
            # Write the table to shared memory
            write_success = shared_memory.write(table)
            
            if not write_success:
                print(f"[{datetime.now().isoformat()}] [Server] WRITE FAILED | Will retry")
                # You might want to implement retry logic here
            
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] [Server] ERROR | {str(e)}")
            raise

    def __del__(self):
        print(f"[{datetime.now().isoformat()}] [FlightServer] CLEANUP | Resources released")
        try:
            if hasattr(self, 'shm'):
                self.shm.close()
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] [FlightServer] CLEANUP ERROR | {str(e)}")
