# webSocketServer.py
import threading
import queue
import multiprocessing.shared_memory
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from simple_reader import SharedMemoryResources
from metadata import systemMetadata
import asyncio
from datetime import datetime
import pyarrow as pa
import json
import uvicorn

app = FastAPI()

# CORS (if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global state
system_metadata = systemMetadata()
shm: SharedMemoryResources = None  # will be set in start_websocket_server
event_queue: queue.Queue = queue.Queue()

def blocking_consumer(shm: SharedMemoryResources, q: queue.Queue):
    """
    Runs in a background thread, blocking on shm.read(),
    and enqueues each non-None pa.Table into a thread-safe queue.
    """
    while True:
        
        try:
            evt = shm.read()
            print("hello",type(evt))
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [ConsumerThread] Read error: {e}")
            evt = None
        if isinstance(evt, pa.Table):
            q.put(evt)
            print(q.qsize())
        # avoid a tight busy loop
        

async def broadcast_queue(q: queue.Queue):
    """
    Async task: pulls from the thread-safe queue via run_in_executor
    and broadcasts each event to all subscribers of its topic.
    """
    loop = asyncio.get_event_loop()
    while True:
        # This will block only the thread in run_in_executor, not the event loop.
        evt = await loop.run_in_executor(None, q.get)
        try:
            topic = evt.schema.metadata[b"topic"].decode()
            payload = json.dumps(evt.to_pydict(), default=str)
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [Broadcast] Invalid event: {e}")
            continue

        subs = system_metadata.getSubscribers(topic)
        for ws in subs:
            try:
                await ws.send_text(payload)
            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [Broadcast] Send error: {e}")
                system_metadata.removeConsumer(topic, ws)

@app.websocket("/ws/{topic}")
async def websocket_handler(websocket: WebSocket, topic: str):
    await websocket.accept()
    system_metadata.addTopic(topic)
    system_metadata.addConsumer(topic, websocket)
    print(f"[{datetime.now().isoformat()}] [WebSocket] Client connected on topic '{topic}'")
    try:
        # Keep this handler alive until disconnect:
        while True:
            # We don't actually care about client messages,
            # but awaiting receive_text() lets us catch a disconnect.
            await websocket.receive_text()
    except WebSocketDisconnect:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Client disconnected")
    finally:
        system_metadata.removeConsumer(topic, websocket)

@app.on_event("startup")
async def startup_event():
    global shm, event_queue
    if shm is not None:
        # Start the blocking consumer thread
        t = threading.Thread(target=blocking_consumer, args=(shm, event_queue), daemon=True)
        t.start()
        # Start the async broadcast task
        asyncio.create_task(broadcast_queue(event_queue))
        print(f"[{datetime.now().isoformat()}] [WebSocket] Consumer thread & broadcaster started")
    else:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Shared memory NOT initialized on startup")

def start_websocket_server(shared_memory_name, lock, write_index, read_index,
                           data_section_start, write_data_idx, read_data_idx,
                           event, event2,header_size,buffer_size, host="0.0.0.0", port=8765):

    global shm
    shm_raw = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
    shm = SharedMemoryResources(
        shm_raw, lock, write_index, read_index,
        data_section_start, write_data_idx, read_data_idx,
        event, event2,header_size,buffer_size
    )

    print(f"[{datetime.now().isoformat()}] [WebSocket] Starting server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
