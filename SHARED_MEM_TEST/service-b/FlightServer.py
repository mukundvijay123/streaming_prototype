import pyarrow as pa
import pyarrow.flight as flight
import multiprocessing.shared_memory
from datetime import datetime
from SharedMemoryResources import SharedMemoryResources
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("flight_server")

class FlightServer(flight.FlightServerBase):
    def __init__(self, shared_memory_name, lock, write_index, read_index, data_section_start, 
                write_data_idx, read_data_idx, location, event, event2, header_size, buffer_size):
        super().__init__(location)
        self.shm_name = shared_memory_name
        self.event = event
        self.event2 = event2  # Make sure this is stored as an attribute
        self.lock = lock
        self.write_index = write_index
        self.read_index = read_index
        self.data_section_start = data_section_start
        self.write_data_idx = write_data_idx
        self.read_data_idx = read_data_idx
        
        # Initialize shared memory
        try:
            self.shm = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
            self.data_section_size = self.shm.size - self.data_section_start.value
            self.header_size = header_size
            self.buffer_size = buffer_size
            logger.info(f"FlightServer initialized with shared memory: {shared_memory_name}")
        except Exception as e:
            logger.error(f"Error initializing shared memory in FlightServer: {e}")
            # Set a flag to indicate initialization failure
            self.init_failed = True
            self.shm = None
        else:
            self.init_failed = False

    def do_put(self, context, descriptor, reader, flight_writer):
        try:
            # Process chunks until we run out
            try:
                while True:
                    try:
                        chunk = reader.read_chunk()
                        if chunk is None:  # This is still a valid check
                            logger.info("Received None chunk, breaking")
                            break
                            
                        logger.info(f"Processing chunk: {chunk}")
                        
                        record_batch = chunk.data
                        schema_metadata = record_batch.schema.metadata

                        if schema_metadata:
                            decoded_metadata = {k.decode(): v.decode() for k, v in schema_metadata.items()}
                            logger.info(f"RecordBatch Metadata: {decoded_metadata}")
                        else:
                            logger.info("No metadata found in record batch.")

                        table = pa.Table.from_batches([record_batch])
                        table_metadata = table.schema.metadata
                        if table_metadata:
                            decoded_table_metadata = {k.decode(): v.decode() for k, v in table_metadata.items()}
                            logger.info(f"Table Metadata: {decoded_table_metadata}")
                        else:
                            logger.info("No metadata found in table.")
                        
                        # Create a new SharedMemoryResources for each operation
                        try:
                            print(chunk)
                            shared_memory = SharedMemoryResources(
                                multiprocessing.shared_memory.SharedMemory(name=self.shm_name),
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
                                logger.warning(f"WRITE FAILED | Will retry")
                                # Implement retry logic here if needed
                            
                            # Don't close the shared memory here as it might be needed for subsequent operations
                            # shared_memory.shm.close()
                            
                        except Exception as e:
                            logger.error(f"Error in do_put while writing to shared memory: {e}")
                            raise
                            
                    except StopIteration:
                        logger.info("StopIteration encountered, finished reading chunks")
                        break
            except Exception as e:
                logger.error(f"Error processing chunks: {e}")
                import traceback
                logger.error(traceback.format_exc())
                raise
                
        except Exception as e:
            logger.error(f"ERROR in do_put: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    def __del__(self):
        logger.info("FlightServer cleanup | Releasing resources")
        try:
            # Check if shm exists and was properly initialized
            if hasattr(self, 'shm') and self.shm is not None:
                logger.info(f"Closing shared memory: {self.shm_name}")
                self.shm.close()
        except Exception as e:
            logger.error(f"CLEANUP ERROR in FlightServer.__del__: {str(e)}")