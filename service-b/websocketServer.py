# websocket.py
import asyncio
from datetime import datetime
import multiprocessing.shared_memory
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import json
import multiprocessing
from event_consumer import consumer  # Import your custom consumer function
from datetime import datetime
import multiprocessing.shared_memory
import pyarrow as pa
import pyarrow.flight as flight
BUFFER_SIZE = 10000  # Max number of messages
HEADER_SIZE = 16  # 8 bytes for size, 8 bytes for offset
DATA_SECTION_SIZE = 1024 * 1024 * 100  # 100MB for data section
app = FastAPI()

# Enable CORS for all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Set of active WebSocket connections
connected_clients = set()

# Store shared resources for the consumer
shared_resources = None

async def consumer_task():
    """
    Continuously consumes events and broadcasts to clients.
    """
    global shared_resources
    print(f"[{datetime.now().isoformat()}] [Consumer] Starting consumer task")
    shm = multiprocessing.shared_memory.SharedMemory(name=shared_resources["name"])
    current_time = datetime.now().strftime('%H:%M:%S')
    
    lock = shared_resources["lock"]
    read_index = shared_resources["read_index"]
    write_index = shared_resources["write_index"]
    data_section_start = shared_resources["data_section_start"]
    read_data_idx = shared_resources["read_data_idx"]
    write_data_idx = shared_resources["write_data_idx"]
    event = shared_resources["event"]
    while True:
        # Call consumer function
        event.wait()
        event_counter = 0
        data_section_size = shm.size - data_section_start.value
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
            else:
                print(f"[{current_time}] [Consumer] WAITING | No new events")
                result = None
            event_data = result
            event_str = json.dumps(event_data, default=str)
            # If we got data, broadcast it
            if event_data is not None:
                # Convert dict to JSON string
                print(f"[{datetime.now().isoformat()}] [Consumer] Broadcasting event")
                
                # Send to all connected clients
                for client in list(connected_clients):
                    try:
                        await client.send_text(event_str)
                    except Exception as e:
                        print(f"[{datetime.now().isoformat()}] [WebSocket] Error sending: {e}")
                        connected_clients.discard(client)
                
                print(f"[{datetime.now().isoformat()}] [Consumer] Event sent to {len(connected_clients)} clients")
            
            event.clear()
            await asyncio.sleep(0.5)



@app.websocket("/ws")
async def websocket_handler(websocket: WebSocket):
    """
    Handles WebSocket connections.
    """
    await websocket.accept()
    connected_clients.add(websocket)
    print(f"[{datetime.now().isoformat()}] [WebSocket] Client connected")

    try:
        # Keep connection alive and handle incoming messages
        while True:
            message = await websocket.receive_text()
            print(f"[{datetime.now().isoformat()}] [WebSocket] Received: {message}")
            # Process client messages if needed
    except WebSocketDisconnect:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Client disconnected")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Error: {str(e)}")
    finally:
        connected_clients.discard(websocket)


@app.on_event("startup")
async def startup_event():
    """
    Start the consumer task when the FastAPI app starts.
    """
    global shared_resources
    if shared_resources is not None:
        asyncio.create_task(consumer_task())
        print(f"[{datetime.now().isoformat()}] [WebSocket] Consumer task started")


def start_websocket_server(shared_memory_name, lock, write_index, read_index, 
                          data_section_start, write_data_idx, read_data_idx, 
                          events, host="0.0.0.0", port=8765):
    """
    Starts the WebSocket server.
    """
    global shared_resources
    
    # Store the references to shared resources
    shared_resources = {
        "name": shared_memory_name,
        "lock": lock,
        "write_index": write_index,
        "read_index": read_index,
        "data_section_start": data_section_start,
        "write_data_idx": write_data_idx,
        "read_data_idx": read_data_idx,
        "event": events
    }
    
    print(f"[{datetime.now().isoformat()}] [WebSocket] Starting server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
