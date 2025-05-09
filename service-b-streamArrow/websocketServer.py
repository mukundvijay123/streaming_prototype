# webSocketServer.py
import threading
import queue
import multiprocessing.shared_memory
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from SharedMemoryResources import SharedMemoryResources
from query_metadata import systemQueryMetadata
import asyncio
from datetime import datetime
import pyarrow as pa
import json
import uvicorn
import aiohttp
from HttpRequests import fetchSubstraitPlan
from clientUtils import subscribe, unsubscribe
import urllib.parse

app = FastAPI()

# CORS (if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

broker_addr = 'grpc://127.0.0.1:8815'
my_addr = 'grpc://127.0.0.1:8816'
query_server_address = 'http://127.0.0.1:8080'

# Global state
system_metadata = systemQueryMetadata(broker_addr, my_addr, query_server_address)
shm: SharedMemoryResources = None
httpClientSession = None


def blocking_consumer(shm: SharedMemoryResources):
    """
    Runs in a background thread, blocking on shm.read(),
    and enqueues each non-None pa.Table into a thread-safe queue.
    """
    while True:
        try:
            evt = shm.read()
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [ConsumerThread] Read error: {e}")
            evt = None
        if isinstance(evt, pa.Table):
            topic = evt.schema.metadata[b'topic'].decode()
            subs = system_metadata.readTopicSubscribers(topic)
            for sub in subs:
                sub.addEvent(evt)

        # avoid a tight busy loop


async def broadcast_task():
    while True:
        # This will block only the thread in run_in_executor, not the event loop.
        evt = await system_metadata.outboundQueue.async_q.get()

        try:
            querySession = evt.schema.metadata[b"queryContext"].decode()
            payload = json.dumps(evt.to_pydict(), default=str)
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [Broadcast] Invalid event: {e}")
            continue

        ctx = system_metadata.getQueryCtx(querySession)
        if ctx:
            try:
                await ctx.sendEvent(payload)
            except:
                print(f"[{datetime.now():%H:%M:%S}] [Broadcast] Send failed: {e}")


@app.websocket("/ws")
async def websocket_handler(websocket: WebSocket):
    await websocket.accept()
    sessionName = None
    try:
        while True:
            # Receive messages from the client
            message = await websocket.receive_json()
            action = message.get("action")

            if action == "start_websocket_connection":
                # Handle WebSocket connection initialization
                await websocket.send_json({"message": "WebSocket connection successfully established."})

            elif action == "get_substrait_plan":
                query = message.get("query")
                if not query:
                    await websocket.send_json({"error": "Query string cannot be empty."})
                    continue

                try:
                    # Forward the query to the Java server
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            f"{query_server_address}/getSubstraitPlan",
                            json={"query": query},
                        ) as response:
                            if response.status != 200:
                                error_message = await response.json()
                                await websocket.send_json({"error": error_message.get("error", "Failed to fetch Substrait plan.")})
                                continue

                            # Send the Substrait plan back to the client
                            substrait_plan = await response.json()
                            await websocket.send_json({"action": "substrait_plan", "plan": substrait_plan["plan"]})

                except Exception as e:
                    print(f"[{datetime.now().isoformat()}] [WebSocket] Error fetching Substrait plan: {e}")
                    await websocket.send_json({"error": "An error occurred while fetching the Substrait plan."})

            elif action == "start_query_session":
                query_string = message.get("query_string")
                query_plan = await fetchSubstraitPlan(query_string, query_server_address, httpClientSession)
                if not query_string:
                    sessionName = system_metadata.createQuerySession(query_string, websocket, False, query_plan)
            elif action == "close":
                system_metadata.deleteQuerySession(sessionName)
                sessionName = None
            elif action == "execute_query":
                # Handle query execution logic if needed
                await websocket.send_json({"message": "Query execution initiated."})
            elif action == "start_query":
                query = message.get("query")
                if not query:
                    await websocket.send_json({"error": "Query string cannot be empty."})
                    continue

                try:
                    # Sanitize the query string
                    sanitized_query = query.replace('\\"', '"')

                    # Fetch the Substrait plan from the Java server
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            f"{query_server_address}/getSubstraitPlan",
                            json={"query": sanitized_query},
                        ) as response:
                            if response.status != 200:
                                error_message = await response.json()
                                await websocket.send_json({"error": error_message.get("error", "Failed to fetch Substrait plan.")})
                                continue

                            # Send the Substrait plan to the client for display
                            substrait_plan = await response.json()
                            await websocket.send_json({"action": "substrait_plan", "plan": substrait_plan["plan"]})

                            # Wait for client confirmation to execute the query
                            confirmation = await websocket.receive_json()
                            if confirmation.get("action") == "execute_query":
                                # Execute the query (placeholder logic)
                                await websocket.send_json({"message": "Query execution initiated."})

                except Exception as e:
                    print(f"[{datetime.now().isoformat()}] [WebSocket] Error fetching Substrait plan: {e}")
                    await websocket.send_json({"error": "An error occurred while fetching the Substrait plan."})

            else:
                await websocket.send_json({"error": f"Unknown action: {action}"})

    except WebSocketDisconnect:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Client disconnected")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Error: {e}")
    finally:
        if sessionName:
            system_metadata.deleteQuerySession(sessionName)


@app.on_event("startup")
async def startup_event():
    global shm, system_metadata, httpClientSession
    httpClientSession = aiohttp.ClientSession()
    if shm is not None:
        # Start the blocking consumer thread
        t = threading.Thread(target=blocking_consumer, args=(shm,), daemon=True)
        t.start()
        # Start the async broadcast task
        asyncio.create_task(broadcast_task())
        print(f"[{datetime.now().isoformat()}] [WebSocket] Consumer thread & broadcaster started")
    else:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Shared memory NOT initialized on startup")


def start_websocket_server(shared_memory_name, lock, write_index, read_index,
                           data_section_start, write_data_idx, read_data_idx,
                           event, event2, header_size, buffer_size, host="0.0.0.0", port=8765):
    global shm
    shm_raw = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
    shm = SharedMemoryResources(
        shm_raw, lock, write_index, read_index,
        data_section_start, write_data_idx, read_data_idx,
        event, event2, header_size, buffer_size
    )

    print(f"[{datetime.now().isoformat()}] [WebSocket] Starting server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
