# event_consumer.py
from datetime import datetime
import multiprocessing.shared_memory
import pyarrow as pa
import pyarrow.flight as flight
from time import sleep

BUFFER_SIZE = 10000  # Max number of messages
HEADER_SIZE = 16  # 8 bytes for size, 8 bytes for offset
DATA_SECTION_SIZE = 1024 * 1024 * 100  # 100MB for data section

def consumer(shared_memory_name, lock, write_index, read_index, data_section_start,
             write_data_idx, read_data_idx, event):
    """
    Consumes events from shared memory.
    """
    # Access the shared memory by name
    shm = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
    event_counter = 0
    data_section_size = shm.size - data_section_start.value
    
    current_time = datetime.now().strftime('%H:%M:%S')
    print(f"[{current_time}] [Consumer] Looking for data...")
    
    # WAIT FOR AN EVENT TO OCCUR
    with lock:
        if read_index.value != write_index.value:
            # Read header (size + offset)
            header_pos = read_index.value * HEADER_SIZE
            header = shm.buf[header_pos:header_pos + HEADER_SIZE]
            message_size = int.from_bytes(header[:8], 'little')
            message_offset = int.from_bytes(header[8:], 'little')
            
            # Calculate actual position in shared memory
            actual_offset = data_section_start.value + message_offset
            
            # Handle wrap-around for data reading
            if message_offset + message_size > data_section_size:
                # Data wraps around the end of the buffer
                first_chunk_size = data_section_size - message_offset
                second_chunk_size = message_size - first_chunk_size
                
                # Read both chunks and combine
                first_chunk = bytes(shm.buf[actual_offset:actual_offset + first_chunk_size])
                second_chunk = bytes(shm.buf[data_section_start.value:data_section_start.value + second_chunk_size])
                message_data = first_chunk + second_chunk
                
                print(f"[{current_time}] [Consumer] WRAPPED READ | First: {first_chunk_size}, Second: {second_chunk_size}")
            else:
                # Normal read
                message_data = bytes(shm.buf[actual_offset:actual_offset + message_size])
            
            # Deserialize Arrow table
            reader = pa.ipc.open_stream(message_data)
            table = reader.read_all()
            
            # Update read_data_idx to indicate we've processed this data
            old_read_data_idx = read_data_idx.value
            if message_offset + message_size > data_section_size:
                # Wrapped read case
                read_data_idx.value = second_chunk_size
            else:
                read_data_idx.value = (message_offset + message_size) % data_section_size
            
            event_counter += 1
            print(f"\n[{current_time}] [Consumer] NEW EVENT #{event_counter}")
            print(f"[{current_time}] [Consumer] MESSAGE SIZE | {message_size} bytes")
            print(f"[{current_time}] [Consumer] DATA READ POS | {old_read_data_idx}→{read_data_idx.value}")
            print(f"[{current_time}] [Consumer] FIRST ROW | {table.slice(0, 1).to_pydict()}")
            
            # Update read index
            old_read_index = read_index.value
            read_index.value = (read_index.value + 1) % BUFFER_SIZE
            print(f"[{current_time}] [Consumer] INDEX UPDATE | {old_read_index}→{read_index.value}")
            
            # Calculate buffer usage
            buffer_usage = (write_index.value - read_index.value) % BUFFER_SIZE
            print(f"[{current_time}] [Consumer] BUFFER STATUS | {buffer_usage}/{BUFFER_SIZE} slots used")
            
            # Convert to Python dict for JSON serialization
            result = table.to_pydict()
            return result
        else:
            print(f"[{current_time}] [Consumer] WAITING | No new events")
            return None
