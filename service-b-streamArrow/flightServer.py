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
        self.shared_memory=SharedMemoryResources(
            multiprocessing.shared_memory.SharedMemory(name=shared_memory_name),#THIS IS THR NAME
                lock,
                write_index,
                read_index,
                data_section_start,
                write_data_idx,
                read_data_idx,
                event,
                event2,
                header_size,
                buffer_size
        )
    def do_put(self, context, descriptor, reader, flight_writer):
        try:
            table = reader.read_all()           
            
            # Write the table to shared memory
            write_success = self.shared_memory.write(table)
            
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
