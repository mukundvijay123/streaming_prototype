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
    
    while True:
        # Call consumer function
        event_data = consumer(
            shared_resources["name"],
            shared_resources["lock"],
            shared_resources["write_index"],
            shared_resources["read_index"],
            shared_resources["data_section_start"],
            shared_resources["write_data_idx"],
            shared_resources["read_data_idx"],
            shared_resources["event"]
        )
        print(event_data)
        # If we got data, broadcast it
        if event_data is not None:
            # Convert dict to JSON string
            event_str = json.dumps(event_data)
            print(f"[{datetime.now().isoformat()}] [Consumer] Broadcasting event")
            
            # Send to all connected clients
            for client in list(connected_clients):
                try:
                    await client.send_text(event_str)
                except Exception as e:
                    print(f"[{datetime.now().isoformat()}] [WebSocket] Error sending: {e}")
                    connected_clients.discard(client)
            
            print(f"[{datetime.now().isoformat()}] [Consumer] Event sent to {len(connected_clients)} clients")
        
        await asyncio.sleep(1)


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