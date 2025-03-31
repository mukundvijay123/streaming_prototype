import asyncio
import websockets
from datetime import datetime
from event_consumer import consumer

# A set to store all active WebSocket connections
connected_clients = set()

async def websocket_handler(websocket, path, shared_memory_name, lock, write_index, read_index, BUFFER_SIZE, EVENT_SIZE):
    # Add the new client to the set of connected clients
    connected_clients.add(websocket)
    print(f"[{datetime.now().isoformat()}] [WebSocket] CLIENT CONNECTED | {websocket.remote_address}")
    
    try:
        while True:
            # Use the existing consumer function to fetch events
            event_str = consumer(shared_memory_name, lock, write_index, read_index, BUFFER_SIZE, EVENT_SIZE)
            
            if event_str:
                # Broadcast the event to all connected clients
                await asyncio.gather(
                    *[client.send(event_str) for client in connected_clients if client.open]
                )
                print(f"[{datetime.now().isoformat()}] [WebSocket] EVENT BROADCASTED TO CLIENTS | {event_str[:100]}...")
                for client in connected_clients:
                    print(f"[{datetime.now().isoformat()}] [WebSocket] CLIENT | {client.remote_address}")
            else:
                # No new events, wait briefly
                print(f"[{datetime.now().isoformat()}] [WebSocket] NO NEW EVENTS TO BROADCAST")
                await asyncio.sleep(0.1)
    except websockets.exceptions.ConnectionClosed:
        print(f"[{datetime.now().isoformat()}] [WebSocket] CLIENT DISCONNECTED | {websocket.remote_address}")
    finally:
        # Remove the client from the set when it disconnects
        connected_clients.remove(websocket)

def start_websocket_server(shared_memory_name, lock, write_index, read_index, BUFFER_SIZE, EVENT_SIZE):
    async def handler(websocket, path):
        await websocket_handler(websocket, path, shared_memory_name, lock, write_index, read_index, BUFFER_SIZE, EVENT_SIZE)

    print(f"[{datetime.now().isoformat()}] [WebSocket] SERVER STARTING | Port 8765")
    try:
        asyncio.run(
            websockets.serve(handler, "0.0.0.0", 8765)
        )
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] [WebSocket] SERVER ERROR | {str(e)}")
        raise
