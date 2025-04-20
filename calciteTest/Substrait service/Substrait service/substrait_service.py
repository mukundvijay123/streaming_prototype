from fastapi import FastAPI, Request, HTTPException
import pyarrow as pa
import pyarrow.substrait as substrait
import logging
import json
import asyncio
import httpx
from contextlib import asynccontextmanager

# Configure logging for debugging
logging.basicConfig(level=logging.DEBUG)

# Store client-substrait mappings
client_plan_map = {}
connected_clients = set()

employees_schema = pa.schema([
    ("id", pa.int32()),
    ("name", pa.string()),
    ("salary", pa.int32())
])

async def send_data_task():
    """Background task that continuously sends data to connected clients."""
    import random
    while True:
        try:
            if not connected_clients:
                await asyncio.sleep(1)  # Sleep if no clients connected
                continue

            # Generate random employee data
            ids = list(range(1, random.randint(1, 100)))
            names = [f"Employee{i}" for i in ids]
            salaries = [20000 + (i % 20) * 1000 for i in ids]

            employees_table = pa.table([
                pa.array(ids),
                pa.array(names),
                pa.array(salaries)
            ], schema=employees_schema)

            def table_provider(named_table, schema):
                return employees_table

            # Process for each connected client
            for client_id in list(connected_clients):  # Create a copy to avoid modification during iteration
                try:
                    if client_id not in client_plan_map:
                        continue

                    json_bytes = json.dumps(client_plan_map[client_id]).encode("utf-8")
                    logging.debug("CONVERTED TO BINARY JSON BYTES for client %s", client_id)
                    buf = pa._substrait._parse_json_plan(json_bytes)
                
                    # Run query
                    reader = substrait.run_query(
                        plan=buf,
                        table_provider=table_provider
                    )
                    result_table = reader.read_all()
                    result = result_table.to_pylist()
                    logging.debug("Result for client %s: %s", client_id, result)
                    
                    # Send result back to client's endpoint
                    async with httpx.AsyncClient() as client:
                        response = await client.post(
                            client_id,
                            json={"result": result},
                            timeout=30.0
                        )
                        
                        if response.status_code != 200:
                            logging.error("Failed to send result to client %s: %s", client_id, response.text)
                            connected_clients.discard(client_id)  # Remove client on failure

                except Exception as e:
                    logging.error("Error processing client %s: %s", client_id, str(e))
                    connected_clients.discard(client_id)  # Remove client on error

            await asyncio.sleep(1)  # Throttle the data generation

        except Exception as e:
            logging.error("Error in send_data_task: %s", str(e))
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the background task when the app starts."""
    task = asyncio.create_task(send_data_task())
    yield
    # Clean up when app shuts down
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)


@app.post("/run-substrait")
async def run_substrait_plan(request: Request):
    try:
        data = await request.json()
        client_id = data.get('clientAddress')
        substrait_plan = data.get('plan')
        
        if not client_id:
            raise HTTPException(status_code=400, detail="clientAddress is required")
        
        if not substrait_plan:
            raise HTTPException(status_code=400, detail="plan is required")
        
        # Store plan for the client
        client_plan_map[client_id] = substrait_plan
        connected_clients.add(client_id)
        
        logging.debug("Stored plan for client %s", client_id)
        return {"status": "success", "message": "Plan registered and client connected"}

    except Exception as e:
        logging.error("Error processing request: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/disconnect")
async def disconnect_client(request: Request):
    try:
        data = await request.json()
        client_id = data.get('clientAddress')
        
        if not client_id:
            raise HTTPException(status_code=400, detail="clientAddress is required")
        
        if client_id in connected_clients:
            connected_clients.remove(client_id)
            logging.debug("Client %s disconnected", client_id)
        
        if client_id in client_plan_map:
            del client_plan_map[client_id]
            logging.debug("Plan removed for client %s", client_id)
        
        return {"status": "success", "message": "Client disconnected"}

    except Exception as e:
        logging.error("Error disconnecting client: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/client-plans")
async def list_client_plans():
    """Optional endpoint to list all stored plans per client."""
    return {
        "connected_clients": list(connected_clients),
        "client_plans": list(client_plan_map.keys())
    }