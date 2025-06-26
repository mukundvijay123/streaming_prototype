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
from clientUtils import subscribe, unsubscribe, check_access_async,clientCtx
import utils

app = FastAPI()

# CORS (if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


broker_addr='grpc://127.0.0.1:8815'
my_addr='grpc://127.0.0.1:8817'
query_server_address='http://127.0.0.1:8080'

# Global state
system_metadata = systemQueryMetadata(broker_addr,my_addr,query_server_address)
shm: SharedMemoryResources = None  


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
            topic=evt.schema.metadata[b'topic'].decode()
            subs=system_metadata.readTopicSubscribers(topic)
            for sub in subs:
                sub.addEvent(evt)
            

        # avoid a tight busy loop
        

async def broadcast_task():

    while True:
        # This will block only the thread in run_in_executor, not the event loop.
        evt =await system_metadata.outboundQueue.async_q.get()

        try:
            querySession= evt.schema.metadata[b"queryContext"].decode()
            payload = json.dumps(evt.to_pydict(), default=str)
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [Broadcast] Invalid event: {e}")
            continue

        ctx= system_metadata.getQueryCtx(querySession)
        if ctx:
            try:
                await ctx.sendEvent(payload)
            except:
                 print(f"[{datetime.now():%H:%M:%S}] [Broadcast] Send failed: {e}")



@app.websocket("/ws")
async def websocket_handler(websocket: WebSocket):
    await websocket.accept()
    sessionName=None
    try:
        while True:
            # Receive subscription requests from the client
            message = await websocket.receive_json()
            print(message)
            action=message.get("action")
            print(action)
            
            if action=="start_query_session":
                query_string=message.get("query_string")
                token = message.get("token")
                query_plan =await fetchSubstraitPlan(query_string,query_server_address)
                if query_string and token:
                    # RBAC: check access for the topic
                    topics = utils.find_topics(query_plan)
                    topic = topics[0] if topics else None
                    context=clientCtx(token)
                    if topic:
                        allowed = await check_access_async("http://localhost:8081/check", context.token, topic, context.action)
                        if not allowed:
                            await websocket.send_text("Error: Not authorized to subscribe to topic.")
                            continue
                    sessionName=system_metadata.createQuerySession(query_string,websocket,False,query_plan,context)
                else:
                    await websocket.send_text("Error: query_string and token required.")
            elif action=="close_query_session":
                system_metadata.deleteQuerySession(sessionName)
                sessionName=None


    except WebSocketDisconnect:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Client disconnected")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] [WebSocket] Error: {e}")
    finally:
        if sessionName:
            system_metadata.deleteQuerySession(sessionName)
        







@app.on_event("startup")
async def startup_event():
    global shm,system_metadata
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
                           event, event2,header_size,buffer_size, host="0.0.0.0", port=8766):

    global shm
    shm_raw = multiprocessing.shared_memory.SharedMemory(name=shared_memory_name)
    shm = SharedMemoryResources(
        shm_raw, lock, write_index, read_index,
        data_section_start, write_data_idx, read_data_idx,
        event, event2,header_size,buffer_size
    )

    print(f"[{datetime.now().isoformat()}] [WebSocket] Starting server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)