from fastapi import FastAPI, Request, HTTPException
import pyarrow as pa
import pyarrow.substrait as substrait
import logging
import json
import asyncio
import httpx
from contextlib import asynccontextmanager
import threading
import queue
from collections import defaultdict
import traceback

# Configure logging for debugging
logging.basicConfig(level=logging.DEBUG, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("substrait_service")

# Store client-substrait mappings and global shared memory resource
client_plan_map = {}
connected_clients = set()
shm = None  # Will be set by setup_shared_memory
event_queue = queue.Queue()

# Enhanced topic subscription tracking
# Maps client_id -> {topic -> plan}
client_topic_plan_map = defaultdict(dict)

# Maps topic -> number of subscribed clients
topic_subscription_count = defaultdict(int)

def setup_shared_memory(shared_memory_resource):
    """Set up the shared memory resource for this service"""
    global shm
    shm = shared_memory_resource
    # Start the blocking consumer thread
    t = threading.Thread(target=blocking_consumer, args=(shm, event_queue), daemon=True)
    t.start()
    logger.info(f"SharedMemoryResource initialized and consumer thread started")

def blocking_consumer(shm, q):
    """
    Runs in a background thread, blocking on shm.read(),
    and enqueues each non-None pa.Table into a thread-safe queue.
    """
    from datetime import datetime
    while True:
        try:
            evt = shm.read()
            if evt is not None:
                logger.debug(f"[ConsumerThread] Read event from shared memory")
        except Exception as e:
            logger.error(f"[ConsumerThread] Read error: {e}")
            evt = None
        
        if isinstance(evt, pa.Table):
            try:
                q.put(evt)
                logger.debug(f"[ConsumerThread] Queue size: {q.qsize()}")
                
                # Debug event metadata
                try:
                    if evt.schema.metadata:
                        for key, value in evt.schema.metadata.items():
                            logger.debug(f"[ConsumerThread] Event metadata: {key.decode()} = {value.decode()}")
                except Exception as e:
                    logger.error(f"[ConsumerThread] Error parsing metadata: {e}")
                    
            except Exception as e:
                logger.error(f"[ConsumerThread] Queue put error: {e}")
        
        # Add a small sleep to avoid a tight busy loop
        asyncio.sleep(0.01)

async def process_events(q):
    """
    Async task: pulls from the thread-safe queue and 
    processes each event according to subscribed client plans
    """
    import json
    from datetime import datetime
    from decimal import Decimal
    
    # Custom JSON encoder to handle datetime and Decimal objects
    class CustomJSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, Decimal):
                return float(obj)  # Convert Decimal to float for JSON
            return super().default(obj)
    
    loop = asyncio.get_event_loop()
    while True:
        # This will block only the thread in run_in_executor, not the event loop
        try:
            evt = await loop.run_in_executor(None, q.get)
            
            # Get the topic from event metadata
            try:
                topic = evt.schema.metadata.get(b"topic", b"unknown").decode()
                logger.info(f"Received event on topic: {topic}")
                
                # Debug event content sample
                logger.debug(f"Event schema: {evt.schema}")
                if evt.num_rows > 0:
                    logger.debug(f"First row sample: {evt.slice(0, 1).to_pylist()}")
                
            except Exception as e:
                logger.error(f"Failed to extract topic from event: {str(e)}")
                continue
            
            # Find clients subscribed to this topic
            logger.debug(f"Current topic subscription map: {dict(client_topic_plan_map)}")
            logger.debug(f"Processing event for topic: {topic}")
            
            subscribed_clients = []
            for client_id, topic_plans in client_topic_plan_map.items():
                if topic in topic_plans:
                    subscribed_clients.append(client_id)
            
            logger.info(f"Found {len(subscribed_clients)} clients subscribed to topic {topic}")
            
            # Process the event for each subscribed client
            for client_id in subscribed_clients:
                logger.debug(f"Processing for client: {client_id}, topic: {topic}")
                
                try:
                    # Get the specific plan for this client and topic
                    plan = client_topic_plan_map[client_id][topic]
                    
                    # Process data through the substrait plan
                    logger.debug(f"Running substrait plan for client {client_id}")
                    result = process_substrait_plan(client_id, evt, plan)
                    
                    logger.debug(f"Sending result to client {client_id}")
                    
                    # Convert Arrow table to Python dictionary
                    result_dict = result.to_pydict()
                    
                    # Send result to client with custom JSON encoder
                    async with httpx.AsyncClient() as client:
                        response = await client.post(
                            client_id,
                            content=json.dumps({"result": result_dict, "topic": topic}, cls=CustomJSONEncoder),
                            headers={"Content-Type": "application/json"},
                            timeout=30.0
                        )
                        
                        if response.status_code != 200:
                            logger.error(f"Failed to send result to client {client_id}: {response.text}")
                        else:
                            logger.info(f"Successfully sent result to client {client_id}")
                except Exception as e:
                    logger.error(f"Error processing event for client {client_id}: {str(e)}")
                    logger.error(traceback.format_exc())
        
        except Exception as e:
            logger.error(f"Error in process_events: {str(e)}")
            logger.error(traceback.format_exc())
            await asyncio.sleep(1)  # Sleep on error to avoid tight loop

def process_substrait_plan(client_id, table, plan):
    """
    Process a PyArrow table through a client's substrait plan
    """
    try:
        logger.debug(f"Table schema: {table.schema}")
        target_schema = pa.schema([
            ('timestamp', pa.timestamp('us')),
            ('stock_symbol', pa.string()),
            ('price', pa.decimal128(10, 2)),
            ('volume', pa.int32()),
            ('bid_price', pa.decimal128(10, 2)),
            ('ask_price', pa.decimal128(10, 2)),
            ('spread', pa.decimal128(10, 2))
        ])
        
        # Cast the existing table to the new schema
        cast_table = table.cast(target_schema)
        table = cast_table
        def table_provider(named_table, schema):
            logger.debug(f"Table provider called for {named_table}")
            return table
        
        # Convert plan to JSON bytes
        json_bytes = json.dumps(plan).encode("utf-8")
        logger.debug(f"Plan size: {len(json_bytes)} bytes")
        
        # Parse the plan
        buf = pa._substrait._parse_json_plan(json_bytes)
        
        # Run query
        logger.debug("Executing substrait query")
        reader = substrait.run_query(
            plan=buf,
            table_provider=table_provider
        )
        
        logger.debug("Reading query results")
        result_table = reader.read_all()
        print(result_table)
        # Convert to Python list
        
        
        return result_table
    except Exception as e:
        logger.error(f"Error processing substrait plan: {str(e)}")
        logger.error(traceback.format_exc())
        return {"error": str(e)}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background tasks when the app starts."""
    logger.info("Initializing application lifespan")
    
    # Only start if shared memory is initialized
    if shm is not None:
        # Start processing task
        logger.info("Starting event processing task")
        task = asyncio.create_task(process_events(event_queue))
        yield
        # Clean up when app shuts down
        logger.info("Shutting down event processing task")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    else:
        logger.warning("App started but shared memory not initialized!")
        yield

app = FastAPI(lifespan=lifespan)

@app.post("/run-substrait")
async def run_substrait_plan(request: Request):
    try:
        # Get raw request first for debugging
        raw_body = await request.body()
        
        logger.debug(f"Received raw request body (first 500 bytes): {raw_body[:500]}")
        
        # Parse the JSON data
        data = await request.json()
        logger.info(f"Received run-substrait request with keys: {list(data.keys())}")
        
        client_id = data.get('clientAddress')
        logger.debug(f"Client address: {client_id}")
        
        # Handle both formats: the original single plan format and the new [plan, topic] pairs format
        if 'plan' in data and 'topics' in data:
            # Original format
            logger.info("Using original format (plan + topics)")
            substrait_plan = data.get('plan')
            topics = data.get('topics', [])
            
            if not client_id or not substrait_plan:
                raise HTTPException(status_code=400, detail="clientAddress and plan are required")
            
            # Store general plan for backward compatibility
            client_plan_map[client_id] = substrait_plan
            connected_clients.add(client_id)
            
            # Store per-topic plans
            for topic in topics:
                client_topic_plan_map[client_id][topic] = substrait_plan
                topic_subscription_count[topic] += 1
                logger.info(f"Client {client_id} subscribed to topic: {topic}")
                
        elif 'plans' in data:
            # New format with plan-topic pairs
            logger.info("Using new format (plans as array of [plan, topic] pairs)")
            substrait_plans = data.get('plans', [])
            
            if not client_id:
                raise HTTPException(status_code=400, detail="clientAddress is required")
            
            if not substrait_plans:
                raise HTTPException(status_code=400, detail="plans is required")
            
            # Store client in connected clients
            connected_clients.add(client_id)
            
            # Process all plan-topic pairs
            for plan_entry in substrait_plans:
                if len(plan_entry) != 2:
                    logger.warning(f"Invalid plan entry format: {plan_entry}")
                    continue
                    
                plan, topic = plan_entry
                
                # For backward compatibility with old code
                client_plan_map[client_id] = plan  # Store the last plan
                
                # Store plan for the client and topic
                client_topic_plan_map[client_id][topic] = plan
                
                # Update subscription count for the topic
                topic_subscription_count[topic] += 1
                
                logger.info(f"Client {client_id} subscribed to topic: {topic} with plan")
        else:
            logger.error("Invalid request format - missing both 'plan' and 'plans' fields")
            raise HTTPException(status_code=400, detail="Either 'plan' or 'plans' field is required")
        
        logger.info(f"Successfully registered plans for client {client_id}")
        logger.debug(f"Current topic subscription map: {dict(client_topic_plan_map)}")
        logger.debug(f"Current topic subscription counts: {dict(topic_subscription_count)}")
        
        return {
            "status": "success", 
            "clientAddress": client_id,
            "message": "Plans registered and client connected"
        }

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/disconnect")
async def disconnect_client(request: Request):
    try:
        data = await request.json()
        client_id = data.get('clientAddress')
        
        if not client_id:
            raise HTTPException(status_code=400, detail="clientAddress is required")
        
        logger.info(f"Disconnecting client: {client_id}")
        
        # Remove client from connected clients
        if client_id in connected_clients:
            connected_clients.remove(client_id)
            logger.debug(f"Client {client_id} removed from connected_clients")
        
        # Update topic subscription counts
        if client_id in client_topic_plan_map:
            for topic in client_topic_plan_map[client_id]:
                topic_subscription_count[topic] -= 1
                logger.debug(f"Topic {topic} subscription count decreased to {topic_subscription_count[topic]}")
                
                # Remove topic if count reaches 0
                if topic_subscription_count[topic] <= 0:
                    del topic_subscription_count[topic]
                    logger.debug(f"Topic {topic} removed from subscription count map")
            
            # Remove client's topic-plan mapping
            del client_topic_plan_map[client_id]
            logger.debug(f"Topic plans removed for client {client_id}")
        
        # Also clean up the old mapping if it exists
        if client_id in client_plan_map:
            del client_plan_map[client_id]
            logger.debug(f"Plan removed for client {client_id} from legacy map")
        
        logger.info(f"Client {client_id} successfully disconnected")
        return {"status": "success", "message": "Client disconnected"}

    except Exception as e:
        logger.error(f"Error disconnecting client: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/client-status")
async def list_client_status():
    """List all connected clients and their subscriptions"""
    logger.info("Client status requested")
    
    status = {
        "connected_clients": list(connected_clients),
        "client_topic_plans": {k: list(v.keys()) for k, v in client_topic_plan_map.items()},
        "topic_subscription_counts": dict(topic_subscription_count)
    }
    
    logger.debug(f"Current status: {status}")
    return status