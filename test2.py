import pyarrow as pa
import multiprocessing as mp

# Access shared memory
shm = mp.shared_memory.SharedMemory(name=metadata['shm_name'])
schema = pa.schema_from_json(metadata['schema'])

# Map directly to Arrow buffers
with pa.OSFile(os.fdopen(shm._fd, 'rb')) as source:
    source.seek(0)
    reader = pa.ipc.open_file(source)
    table = reader.read_all()
