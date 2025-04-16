import multiprocessing.shared_memory
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from simple_reader import SharedMemoryResources
from metadata import systemMetadata
import asyncio
from datetime import datetime
import pyarrow as pa
import json
import multiprocessing
import uvicorn

app = FastAPI()

# Enable CORS for all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

system_metadata = systemMetadata()
shm = None  # Will be initialized in start_websocket_server

# Consumer task: repeatedly calls shm.read() and puts the event onto an asyncio.Queue.
async def consumerThread(shm: SharedMemoryResources, qu: asyncio.Queue):
    while True:
        try:
            event = shm.read()
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [Consumer] Error in reading shared memory: {e}")
            event = None
        # If a None or invalid event is received, ignore it.
        if event is not None:
            await qu.put(event)
        

# Broadcast task: pulls events from the queue and sends them to WebSocket subscribers.
async def broadcast(qu: asyncio.Queue):
    while True:
        event = await qu.get()
        # Skip if the event is None or not an Arrow table.
        if event is None or not isinstance(event, pa.Table):
            continue

        try:
            event_topic = event.schema.metadata[b"topic"].decode()
            event_json = json.dumps(event.to_pydict(), default=str)
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [Broadcast] Error fetching topic: {e}")
            continue

        subscribers = system_metadata.getSubscribers(event_topic)
        for subscriber in subscribers:
            try:
                await subscriber.send_text(event_json)
            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [Broadcast] Error sending to subscriber: {e}")
                system_metadata.removeConsumer(event_topic, subscriber)

# WebSocket endpoint
@app.websocket("/ws/{topic}")
async def websocket_handler(websocket: WebSocket, topic: str):
    await websocket.accept()
    system_metadata.addTopic(topic)
    system_metadata.addConsumer(topic, websocket)
    print(f"[{datetime.now().isoformat()}] [WebSocket] Client connected on topic '{topic}'")

    try:
        # Here you can add your protocol logic—for example, waiting for a client message.
        message = await websocket.receive_text()
        print(f"[{datetime.now().isoformat()}] [WebSocket] Received: {message}")
    except WebSocketDisconnect:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Client disconnected")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Error: {e}")
    finally:
        system_metadata.removeConsumer(topic, websocket)

# Startup event: schedule the background consumer and broadcast tasks.
@app.on_event("startup")
async def startup_event():
    qu = asyncio.Queue()
    global shm
    if shm is not None:
        asyncio.create_task(consumerThread(shm, qu))
        asyncio.create_task(broadcast(qu))
        print(f"[{datetime.now().isoformat()}] [WebSocket] Consumer and Broadcast tasks started")
    else:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Shared memory not initialized at startup")

# Function to start the WebSocket server.
def start_websocket_server(shared_memory_name, lock, write_index, read_index,
                           data_section_start, write_data_idx, read_data_idx,
                           event, event2, host="0.0.0.0", port=8765):
    global shm
    shared_memory = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
    shm = SharedMemoryResources(
        shared_memory, lock, write_index, read_index,
        data_section_start, write_data_idx, read_data_idx, event, event2
    )

    print(f"[{datetime.now().isoformat()}] [WebSocket] Starting server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
