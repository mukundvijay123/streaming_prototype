import pyarrow as pa
import pyarrow.flight as flight
import multiprocessing.shared_memory
from datetime import datetime

BUFFER_SIZE = 10000
HEADER_SIZE = 16  # 8 bytes for size, 8 bytes for offset

class FlightServer(flight.FlightServerBase):
    def __init__(self, shared_memory_name, lock, write_index, read_index, data_section_start, 
                 write_data_idx, read_data_idx, location, event):
        super().__init__(location)
        self.shm_name = shared_memory_name
        self.lock = lock
        self.write_index = write_index
        self.read_index = read_index
        self.data_section_start = data_section_start
        self.write_data_idx = write_data_idx
        self.read_data_idx = read_data_idx
        self.event = event

        self.shm = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
        self.data_section_size = self.shm.size - self.data_section_start.value

    def do_put(self, context, descriptor, reader, writer):
        print(f"\nReceiving stream for: {descriptor.path}")
        print("Descriptor:", descriptor)

        batch_number = 0

        try:
            while True:
                chunk = reader.read_chunk()
                if chunk is None:
                    break

                record_batch = chunk.data
                schema_metadata = record_batch.schema.metadata

                if schema_metadata:
                    decoded_metadata = {k.decode(): v.decode() for k, v in schema_metadata.items()}
                    print("RecordBatch Metadata:", decoded_metadata)
                else:
                    print("No metadata found in record batch.")

                table = pa.Table.from_batches([record_batch])
                table_metadata = table.schema.metadata
                if table_metadata:
                    decoded_table_metadata = {k.decode(): v.decode() for k, v in table_metadata.items()}
                    print("Table Metadata:", decoded_table_metadata)
                else:
                    print("No metadata found in table.")

                # Serialize the table
                sink = pa.BufferOutputStream()
                writer_ipc = pa.ipc.new_stream(sink, table.schema)
                writer_ipc.write(table)
                writer_ipc.close()
                serialized = sink.getvalue().to_pybytes()
                message_size = len(serialized)

                with self.lock:
                    buffer_usage = (self.write_index.value - self.read_index.value) % BUFFER_SIZE
                    if buffer_usage == BUFFER_SIZE - 1:
                        raise BufferError("Circular buffer full. Consumer is too slow.")

                    write_pos = self.write_data_idx.value
                    actual_write_pos = self.data_section_start.value + write_pos

                    if self.write_data_idx.value < self.read_data_idx.value:
                        available_space = self.read_data_idx.value - self.write_data_idx.value
                        if message_size > available_space:
                            raise BufferError("Not enough space in buffer.")

                        self.shm.buf[actual_write_pos:actual_write_pos + message_size] = serialized
                        new_data_pos = write_pos + message_size
                    else:
                        space_to_end = self.data_section_size - write_pos
                        if message_size <= space_to_end:
                            self.shm.buf[actual_write_pos:actual_write_pos + message_size] = serialized
                            new_data_pos = (write_pos + message_size) % self.data_section_size
                        else:
                            first_chunk_size = space_to_end
                            second_chunk_size = message_size - first_chunk_size

                            if second_chunk_size > self.read_data_idx.value:
                                raise BufferError("Not enough wrap-around space.")

                            self.shm.buf[actual_write_pos:actual_write_pos + first_chunk_size] = serialized[:first_chunk_size]
                            self.shm.buf[self.data_section_start.value:self.data_section_start.value + second_chunk_size] = serialized[first_chunk_size:]

                            print(f"[{datetime.now().isoformat()}] WRAPPED WRITE: First={first_chunk_size}, Second={second_chunk_size}")
                            new_data_pos = second_chunk_size

                    # Write header
                    header_pos = self.write_index.value * HEADER_SIZE
                    self.shm.buf[header_pos:header_pos + 8] = message_size.to_bytes(8, 'little')
                    self.shm.buf[header_pos + 8:header_pos + 16] = write_pos.to_bytes(8, 'little')

                    old_write_index = self.write_index.value
                    old_data_pos = self.write_data_idx.value
                    self.write_index.value = (self.write_index.value + 1) % BUFFER_SIZE
                    self.write_data_idx.value = new_data_pos

                    print(f"[{datetime.now().isoformat()}] STORED | Size: {message_size}, "
                          f"Index: {old_write_index}→{self.write_index.value}, "
                          f"Data: {old_data_pos}→{self.write_data_idx.value}, "
                          f"ReadPos: {self.read_data_idx.value}")

                    if self.write_data_idx.value >= self.read_data_idx.value:
                        data_usage = self.write_data_idx.value - self.read_data_idx.value
                    else:
                        data_usage = self.data_section_size - (self.read_data_idx.value - self.write_data_idx.value)

                    print(f"[{datetime.now().isoformat()}] BUFFER USAGE: {data_usage}/{self.data_section_size} "
                          f"({data_usage/self.data_section_size*100:.1f}%)")
                    self.event.set()

                print(f"\nBatch {batch_number}:\n", table)
                batch_number += 1

        except Exception as e:
            print(f"[{datetime.now().isoformat()}] ERROR | {str(e)}")
            raise

    def __del__(self):
        print(f"[{datetime.now().isoformat()}] CLEANUP | Shared memory closed.")
        self.shm.close()
