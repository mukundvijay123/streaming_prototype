from fastapi import FastAPI, Request, HTTPException
import pyarrow as pa
import pyarrow.substrait as substrait
import logging
import json

app = FastAPI()

# Configure logging for debugging
logging.basicConfig(level=logging.DEBUG)

# Store client-substrait mappings
client_plan_map = {}

employees_schema = pa.schema([
    ("id", pa.int32()),
    ("name", pa.string()),
    ("salary", pa.int32())
])

# Generate 100 employees
ids = list(range(1, 101))
names = [f"Employee{i}" for i in ids]
salaries = [20000 + (i % 20) * 1000 for i in ids]  # Salaries range from 40k to 59k

employees_table = pa.table([
    pa.array(ids),
    pa.array(names),
    pa.array(salaries)
], schema=employees_schema)

def table_provider(named_table, schema):
    return employees_table
import httpx
from fastapi import Request, HTTPException
@app.post("/recieve-updated-client-substrait")
async def getUpdate(request: Request):
    return {"status": "success"}
@app.post("/run-substrait")
async def run_substrait_plan(request: Request):
    try:
        data = await request.json()
        client_id = data.get('clientAddress')
        substrait_plan = data.get('plan')
        
        # Print received data (for debugging)
        print("\nReceived data:")
        print(f"Client IP: {client_id}")
        print(f"Query: {data.get('query')}")
        print("Plan:", substrait_plan)
        
        # Store plan for the client
        client_plan_map[client_id] = substrait_plan
        logging.debug("Stored plan for client %s", client_id)

        # Convert to binary plan
        json_bytes = json.dumps(substrait_plan).encode("utf-8")
        print("CONVERTED TO BINARY JSON BYTES")
        buf = pa._substrait._parse_json_plan(json_bytes)
       
        # Run query
        reader = substrait.run_query(
            plan=buf,
            table_provider=table_provider
        )
        result_table = reader.read_all()
        result = result_table.to_pylist()
        print("Result:", result)
        
        # Send result back to client's endpoint
        async with httpx.AsyncClient() as client:
            response = await client.post(
                client_id,  # Assuming client_id is the endpoint URL
                json={"result": result},
                timeout=30.0
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to send result to client: {response.text}"
                )
        
        return {"status": "success", "message": "Result sent to client"}

    except Exception as e:
        logging.error("Error processing request: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/client-plans")
async def list_client_plans():
    """Optional endpoint to list all stored plans per client."""
    return {"clients": list(client_plan_map.keys())}