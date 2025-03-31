import asyncio
from datetime import datetime
import multiprocessing.shared_memory
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import multiprocessing
from event_consumer import consumer  # Import your custom consumer function

app = FastAPI()

# Enable CORS for all origins (if needed for the client-side app)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Set of active WebSocket connections
connected_clients = set()

async def consumer_task(shared_memory_name, lock, write_index, read_index, BUFFER_SIZE, EVENT_SIZE):
    """
    Continuously consumes events and, if any clients are connected, broadcasts the event.
    """
    shared_memory=multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
    try:
        while True:
            event_str = consumer(shared_memory, lock, write_index, read_index, BUFFER_SIZE, EVENT_SIZE)
            if event_str is not None:
                print(f"[{datetime.now().isoformat()}] [Consumer] New event: {event_str[:100]}...")
                # Send event to all connected clients
                for client in connected_clients:
                        try:
                            await client.send_text(event_str)
                        except Exception as e:
                            print(f"[{datetime.now().isoformat()}] [WebSocket] Failed to send: {e}")                
                        print(f"[{datetime.now().isoformat()}] [Consumer] Event broadcast to {len(connected_clients)} client(s)")
            else:
                print(f"[{datetime.now().isoformat()}] [Consumer] No new events")
            await asyncio.sleep(1)  # Brief pause before polling again
    except Exception as e:
        print(e)
    finally:
        shared_memory.close()


@app.websocket("/ws")
async def websocket_handler(websocket: WebSocket):
    """
    Handles incoming WebSocket connections.
    """
    await websocket.accept()
    connected_clients.add(websocket)
    print(f"[{datetime.now().isoformat()}] [WebSocket] Client connected: {websocket.client}")

    try:
        while True:
            message = await websocket.receive_text()
            print(f"[{datetime.now().isoformat()}] [WebSocket] Received: {message}")
    except WebSocketDisconnect:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Client disconnected: {websocket.client}")
    finally:
        connected_clients.discard(websocket)

def start_websocket_server(shared_memory_name, lock, write_index, read_index, BUFFER_SIZE, EVENT_SIZE, host="0.0.0.0", port=8765):
    """
    Starts the WebSocket server with event consumer.
    """
    @app.on_event("startup")
    async def start_background_tasks():
        loop = asyncio.get_running_loop()
        loop.create_task(consumer_task(shared_memory_name, lock, write_index, read_index, BUFFER_SIZE, EVENT_SIZE))

    uvicorn.run(app, host=host, port=port)


