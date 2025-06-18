import pyarrow as pa
import pyarrow.flight as flight
import json

# Connect to the Flight server
location = flight.Location.for_grpc_tcp("localhost", 8815)
client = flight.FlightClient(location)

# Prepare and encode the request
request_data = {
    "topic": "XYZ",
    "offset": 4
}
ticket = flight.Ticket(json.dumps(request_data).encode('utf-8'))

# Send request using do_get
reader = client.do_get(ticket)

# Read each record batch and convert to an individual table
print("Received tables:")
for record_batch in reader:
    table = pa.Table.from_batches([record_batch])
    print(table)
