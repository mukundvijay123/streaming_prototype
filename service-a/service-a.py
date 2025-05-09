from queueMap import QueueMap
from streamSimulator import streamSimulator
from broadcast import FlightBroadcaster
from metadata import systemMetadata
from scheduler import Scheduler
from flightServer import FlightServer
import threading
import requests  # Add this import for HTTP requests

JAVA_SERVER_ADDRESS = "http://127.0.0.1:8080"  # Address of the Calcite server

system_metadata=systemMetadata(3)
queue_map=QueueMap()
scheduler=Scheduler(system_metadata,queue_map,5)

           
for _ in range(system_metadata.broadcastThreads):
    broadcastThread=FlightBroadcaster()
    scheduler.AddBroadcastThread(broadcastThread)

streamSimulatorThread=threading.Thread(target=streamSimulator,args=(queue_map,))
streamSimulatorThread.start()

scheduler.start()



def add_topic_to_system(topic_name, schema):
    """
    Sends a request to the Java server to add a topic and schema.
    If successful, adds the topic to system metadata and queue map.
    """
    url = f"{JAVA_SERVER_ADDRESS}/create"
    payload = {
        "topic": topic_name,
        "createTableStatement": schema
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 201:  # Success
            system_metadata.addTopic(topic_name)
            queue_map.add_topic(topic_name)
            print(f"Topic '{topic_name}' successfully added to system metadata and queue map.")
        else:
            print(f"Failed to add topic '{topic_name}': {response.json().get('error', 'Unknown error')}")
    except Exception as e:
        print(f"Error while communicating with Java server: {e}")

# Define schema for topics
schema_template = """
CREATE TABLE IF NOT EXISTS {topic_name} (
        id INTEGER,
        "timestamp" TIMESTAMP NOT NULL,
        stock_symbol VARCHAR(10) NOT NULL,
        price NUMERIC(10, 2) NOT NULL,
        volume INTEGER NOT NULL,
        bid_price NUMERIC(10, 2) NOT NULL,
        ask_price NUMERIC(10, 2) NOT NULL,
        spread NUMERIC(10, 2) NOT NULL
    );
"""

# Add topics to the system
for topic in ["ABC", "XYZ", "LMN"]:
    schema = schema_template.format(topic_name=topic)
    add_topic_to_system(topic, schema)


print(system_metadata.readTopics())

server = FlightServer(system_metadata)
server.serve()



