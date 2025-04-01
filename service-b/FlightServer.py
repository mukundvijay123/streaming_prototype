import pyarrow as pa
import pyarrow.flight as flight
import multiprocessing.shared_memory
from datetime import datetime

BUFFER_SIZE = 10000
HEADER_SIZE = 16  # 8 bytes for size, 8 bytes for offset
#0                   16 * 10K, 
#[HEADER1, HEADER2, ... 10000, DATA1, DATA2, ...]
# R, W                           R_D_I, W_D_I
#[SIZE -> 8 BYTES]
#[OFFSET -> 8 BYTES]
class FlightServer(flight.FlightServerBase):
    def __init__(self, shared_memory_name, lock, write_index, read_index, data_section_start, 
                write_data_idx, read_data_idx, location, event):
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

    def do_put(self, context, descriptor, reader, flight_writer):
        """Handles incoming data from clients"""
        try:
            table = reader.read_all()
            
            # Serialize table to bytes
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, table.schema)
            writer.write(table)
            writer.close()
            serialized = sink.getvalue().to_pybytes()
            message_size = len(serialized)
        
            with self.lock:
                # Check if buffer indices are full
                buffer_usage = (self.write_index.value - self.read_index.value) % BUFFER_SIZE
                if buffer_usage == BUFFER_SIZE - 1:
                    raise BufferError("Circular buffer indices are full. Consumer cannot keep up.")
                
                # Calculate actual position in shared memory
                write_pos = self.write_data_idx.value
                actual_write_pos = self.data_section_start.value + write_pos
                
                # Check space availability based on read position
                #DATA:
                #[....R.........W....]
                #MESSAGE -> MESSAGE1 + MESSAGE2
                #MESSAGE1 => DATA_SECTION_SIZE - WRITE_INDEX => WRITE_INDEX : DATA_SECTION_SIZE
                #MESSAGE2 => 0: LEN(MESSAGE) - LEN(MESSAGE1)
                if self.write_data_idx.value < self.read_data_idx.value:
                    # Case 1: read_data_idx > write_data_idx
                    available_space = self.read_data_idx.value - self.write_data_idx.value
                    if message_size > available_space:
                        raise BufferError(f"Not enough space in buffer. Need {message_size}, have {available_space}")
                    
                    # Write in one chunk (no wrap-around needed)
                    self.shm.buf[actual_write_pos:actual_write_pos + message_size] = serialized
                    new_data_pos = write_pos + message_size
                else:
                    # Case 2 & 3: read_data_idx <= write_data_idx
                    # Try to write after write_pos up to the end of buffer
                    space_to_end = self.data_section_size - write_pos
                    
                    if message_size <= space_to_end:
                        # Case 2: Enough space until end of buffer - no wrap needed
                        self.shm.buf[actual_write_pos:actual_write_pos + message_size] = serialized
                        new_data_pos = (write_pos + message_size) % self.data_section_size
                    else:
                        # Case 3: Need to wrap around
                        # First chunk from write_pos to end of buffer
                        first_chunk_size = space_to_end
                        second_chunk_size = message_size - first_chunk_size
                        
                        # Check if we have enough space after wrap-around
                        if second_chunk_size > self.read_data_idx.value:
                            raise BufferError(f"Not enough space after wrap-around. Need {second_chunk_size}, have {self.read_data_idx.value}")
                        
                        # Write first chunk
                        self.shm.buf[actual_write_pos:actual_write_pos + first_chunk_size] = serialized[:first_chunk_size]
                        
                        # Write second chunk at beginning of data section
                        self.shm.buf[self.data_section_start.value:self.data_section_start.value + second_chunk_size] = serialized[first_chunk_size:]
                        
                        print(f"[{datetime.now().isoformat()}] [Server] WRAPPED WRITE | " 
                              f"First: {first_chunk_size}, Second: {second_chunk_size}")
                        
                        new_data_pos = second_chunk_size
                
                # Write header (size + offset)
                #[HEADER1, HEADER2, HEADER3, .....]
                header_pos = self.write_index.value * HEADER_SIZE
                self.shm.buf[header_pos:header_pos+8] = message_size.to_bytes(8, 'little')
                self.shm.buf[header_pos+8:header_pos+16] = write_pos.to_bytes(8, 'little')
                
                # Update indices
                old_write_index = self.write_index.value
                old_data_pos = self.write_data_idx.value
                self.write_index.value = (self.write_index.value + 1) % BUFFER_SIZE
                self.write_data_idx.value = new_data_pos
                
                print(f"[{datetime.now().isoformat()}] [Server] STORED MESSAGE | "
                      f"Size: {message_size} bytes | "
                      f"Write Index: {old_write_index}→{self.write_index.value} | "
                      f"Data Position: {old_data_pos}→{self.write_data_idx.value} | "
                      f"Read Data Position: {self.read_data_idx.value}")
                
                # Calculate data buffer usage
                if self.write_data_idx.value >= self.read_data_idx.value:
                    data_usage = self.write_data_idx.value - self.read_data_idx.value
                else:
                    data_usage = self.data_section_size - (self.read_data_idx.value - self.write_data_idx.value)
                
                
                print(f"[{datetime.now().isoformat()}] [Server] DATA BUFFER STATUS | "
                      f"{data_usage}/{self.data_section_size} bytes used "
                      f"({data_usage/self.data_section_size*100:.1f}%)")
                self.event.set()
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] [Server] ERROR | {str(e)}")
            raise

    def __del__(self):
        print(f"[{datetime.now().isoformat()}] [FlightServer] CLEANUP | Resources released")
        self.shm.close()