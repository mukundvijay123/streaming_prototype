import pyarrow as pa
import multiprocessing as mp

# Create shared memory buffer
shm = mp.shared_memory.SharedMemory(create=True, size=1024*1024)  # Adjust size

# Build Arrow table directly in shared memory
with pa.OSFile(os.fdopen(shm._fd, 'rb+')) as sink:
    schema = pa.schema([('col1', pa.int32()), ('col2', pa.string())])
    with pa.ipc.new_file(sink, schema) as writer:
        batch = pa.record_batch([
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(["a", "b", "c"], type=pa.string())
        ], schema=schema)
        writer.write(batch)

# Share only metadata (schema + buffer offsets)
metadata = {
    'shm_name': shm.name,
    'schema': schema.to_string(),
    'buffer_offsets': sink.tell()  # Get final position
}
