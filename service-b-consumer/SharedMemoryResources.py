import multiprocessing
from time import sleep
import pyarrow as pa
import pyarrow.flight as flight
import multiprocessing.shared_memory
from datetime import datetime
from clientUtils import subscribe, unsubscribe
from multiprocessing import Event, Lock


class SharedMemoryResources:
    def __init__(self, shm, lock, write_index, read_index, data_section_start, 
                 write_data_idx, read_data_idx, data_available_event, space_available_event,header_size,buffer_size):
        self.shm = shm
        self.lock = lock
        self.write_index = write_index
        self.read_index = read_index
        self.data_section_start = data_section_start
        self.write_data_idx = write_data_idx
        self.read_data_idx = read_data_idx
        self.data_available_event = data_available_event  # Signals data is available to read
        self.space_available_event = space_available_event  # Signals space is available to write
        self.space_available_event.set()  # Initially space is available
        self.data_section_size = self.shm.size - self.data_section_start.value
        self.header_size = header_size
        self.buffer_size = buffer_size

    def read(self):
        """Read from the global shared object"""
        current_time = datetime.now().strftime('%H:%M:%S')
        print(f"[{current_time}] [Consumer] Looking for data...")
        
        # Wait for data to be available
        self.data_available_event.wait()
        
        # Acquire the lock to read safely
        self.lock.acquire()
        try:
            if self.read_index.value != self.write_index.value:
                # Read header (size + offset)
                header_pos = self.read_index.value * self.header_size
                header = self.shm.buf[header_pos:header_pos + self.header_size]
                message_size = int.from_bytes(header[:8], 'little')
                message_offset = int.from_bytes(header[8:], 'little')
                
                # Calculate actual position in shared memory
                actual_offset = self.data_section_start.value + message_offset
                
                # Handle wrap-around for data reading
                if message_offset + message_size > self.data_section_size:
                    # Data wraps around the end of the buffer
                    first_chunk_size = self.data_section_size - message_offset
                    second_chunk_size = message_size - first_chunk_size
                    
                    # Read both chunks and combine
                    first_chunk = bytes(self.shm.buf[actual_offset:actual_offset + first_chunk_size])
                    second_chunk = bytes(self.shm.buf[self.data_section_start.value:self.data_section_start.value + second_chunk_size])
                    message_data = first_chunk + second_chunk
                    
                    print(f"[{current_time}] [Consumer] WRAPPED READ | First: {first_chunk_size}, Second: {second_chunk_size}")
                else:
                    # Normal read
                    message_data = bytes(self.shm.buf[actual_offset:actual_offset + message_size])
                
                try:
                    # Deserialize Arrow table
                    reader = pa.ipc.open_stream(message_data)
                    table = reader.read_all()
                
                    
                    # Update read_data_idx to indicate we've processed this data
                    old_read_data_idx = self.read_data_idx.value
                    if message_offset + message_size > self.data_section_size:
                        # Wrapped read case
                        self.read_data_idx.value = second_chunk_size
                    else:
                        self.read_data_idx.value = (message_offset + message_size) % self.data_section_size
                    
                    print(f"\n[{current_time}] [Consumer] NEW EVENT #1")
                    print(f"[{current_time}] [Consumer] MESSAGE SIZE | {message_size} bytes")
                    print(f"[{current_time}] [Consumer] DATA READ POS | {old_read_data_idx}→{self.read_data_idx.value}")
                    print(f"[{current_time}] [Consumer] FIRST ROW | {table}")
                    
                    # Update read index
                    old_read_index = self.read_index.value
                    self.read_index.value = (self.read_index.value + 1) % self.buffer_size
                    print(f"[{current_time}] [Consumer] INDEX UPDATE | {old_read_index}→{self.read_index.value}")
                    
                    # Calculate buffer usage
                    buffer_usage = (self.write_index.value - self.read_index.value) % self.buffer_size
                    print(f"[{current_time}] [Consumer] BUFFER STATUS | {buffer_usage}/{self.buffer_size} slots used")
                    
                    # Convert to Python dict for JSON serialization
                    result = table
                    
                    # Signal that space is available for writers
                    self.space_available_event.set()
                    
                    return result
                    
                except pa.lib.ArrowInvalid as e:
                    print(f"[{current_time}] [Consumer] ERROR | Arrow deserialization failed: {str(e)}")
                    print(f"[{current_time}] [Consumer] ERROR | Message size: {message_size}, Data length: {len(message_data)}")
                    
                    # Skip this message and move read pointers forward
                    old_read_index = self.read_index.value
                    self.read_index.value = (self.read_index.value + 1) % self.buffer_size
                    self.read_data_idx.value = (message_offset + message_size) % self.data_section_size
                    
                    # Signal that space is available for writers
                    self.space_available_event.set()
                    
                    return None
            else:
                print(f"[{current_time}] [Consumer] WAITING | No new events")
                # If there's no data to read, clear the data available event
                self.data_available_event.clear()
                return None
        finally:
            # Always release the lock when done
            self.lock.release()

    def write(self, table):
        """Writes a PyArrow table to shared memory"""
        current_time = datetime.now().strftime('%H:%M:%S')
        
        try:
            # Serialize table to bytes
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, table.schema)
            writer.write(table)
            writer.close()
            serialized = sink.getvalue().to_pybytes()
            message_size = len(serialized)
            
            while True:  # Keep trying until we succeed
                # Acquire the lock to check and update the buffer
                self.lock.acquire()
                try:
                    # Check if buffer indices are full
                    if (self.write_index.value - self.read_index.value) % self.buffer_size == self.buffer_size - 1:
                        print(f"[{current_time}] [SharedMemory] QUEUE IS FULL, WAITING FOR READER...")
                        # Release the lock before waiting
                        self.lock.release()
                        
                        # Signal the reader that data is available
                        self.data_available_event.set()
                        
                        # Wait for space to become available
                        self.space_available_event.clear()
                        self.space_available_event.wait()
                        
                        # Continue to retry with the lock in the next iteration
                        continue
                    
                    # Calculate actual position in shared memory
                    write_pos = self.write_data_idx.value
                    actual_write_pos = self.data_section_start.value + write_pos
                    
                    # Check space availability based on read position
                    if self.write_data_idx.value < self.read_data_idx.value:
                        # Case 1: read_data_idx > write_data_idx
                        if message_size > self.read_data_idx.value - self.write_data_idx.value:
                            print(f"[{current_time}] [SharedMemory] NOT ENOUGH SPACE, WAITING FOR READER...")
                            # Release the lock before waiting
                            self.lock.release()
                            
                            # Signal the reader that data is available
                            self.data_available_event.set()
                            
                            # Wait for space to become available
                            self.space_available_event.clear()
                            self.space_available_event.wait()
                            
                            # Continue to retry with the lock in the next iteration
                            continue
                        
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
                                print(f"[{current_time}] [SharedMemory] WRAPPED WRITE NEEDS MORE SPACE, WAITING...")
                                # Release the lock before waiting
                                self.lock.release()
                                
                                # Signal the reader that data is available
                                self.data_available_event.set()
                                
                                # Wait for space to become available
                                self.space_available_event.clear()
                                self.space_available_event.wait()
                                
                                # Continue to retry with the lock in the next iteration
                                continue
                            
                            # Write first chunk
                            self.shm.buf[actual_write_pos:actual_write_pos + first_chunk_size] = serialized[:first_chunk_size]
                            
                            # Write second chunk at beginning of data section
                            self.shm.buf[self.data_section_start.value:self.data_section_start.value + second_chunk_size] = serialized[first_chunk_size:]
                            
                            print(f"[{current_time}] [SharedMemory] WRAPPED WRITE | " 
                                f"First: {first_chunk_size}, Second: {second_chunk_size}")
                            
                            new_data_pos = second_chunk_size
                    
                    # Write header (size + offset)
                    header_pos = self.write_index.value * self.header_size
                    self.shm.buf[header_pos:header_pos+8] = message_size.to_bytes(8, 'little')
                    self.shm.buf[header_pos+8:header_pos+16] = write_pos.to_bytes(8, 'little')
                    
                    # Update indices
                    old_write_index = self.write_index.value
                    old_data_pos = self.write_data_idx.value
                    self.write_index.value = (self.write_index.value + 1) % self.buffer_size
                    self.write_data_idx.value = new_data_pos
                    
                    print(f"[{current_time}] [SharedMemory] STORED MESSAGE | "
                        f"Size: {message_size} bytes | "
                        f"Write Index: {old_write_index}→{self.write_index.value} | "
                        f"Data Position: {old_data_pos}→{self.write_data_idx.value} | "
                        f"Read Data Position: {self.read_data_idx.value}")
                    
                    # Calculate data buffer usage
                    if self.write_data_idx.value >= self.read_data_idx.value:
                        data_usage = self.write_data_idx.value - self.read_data_idx.value
                    else:
                        data_usage = self.data_section_size - (self.read_data_idx.value - self.write_data_idx.value)
                    
                    print(f"[{current_time}] [SharedMemory] DATA BUFFER STATUS | "
                        f"{data_usage}/{self.data_section_size} bytes used "
                        f"({data_usage/self.data_section_size*100:.1f}%)")
                    
                    # Signal readers that new data is available
                    self.data_available_event.set()
                    
                    # Successfully wrote the data
                    return True
                    
                finally:
                    # Check if we still have the lock before releasing it
                    # This is needed because in some cases we release it explicitly above
                    if self.lock._semlock._is_mine():
                        self.lock.release()
                
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] [SharedMemory] WRITE ERROR | {str(e)}")
            return False
