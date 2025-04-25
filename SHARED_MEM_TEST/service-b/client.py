from flask import Flask, request, jsonify, Response
from queue import Queue, Empty
from flask_cors import CORS
import requests
import threading
import time
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend access

# Queue for storing data to be displayed on frontend
data_queue = Queue()

# Forward queries to localhost:8080
@app.route('/forward-query', methods=['POST'])
def forward_query():
    # Check if we have valid JSON data
    if not request.is_json:
        return "Request must be in JSON format", 400
    
    try:
        # Parse the JSON data - expecting an array of [query, topic] pairs
        query_pairs = request.get_json()
        if not isinstance(query_pairs, list) or len(query_pairs) == 0:
            return "Invalid data format. Expected non-empty array of query-topic pairs", 400
        
        # Validate the structure of the data
        for pair in query_pairs:
            if not isinstance(pair, list) or len(pair) != 2:
                return "Each pair must contain exactly 2 elements [query, topic]", 400
            if not isinstance(pair[0], str) or not isinstance(pair[1], str):
                return "Query and topic must be strings", 400
            if not pair[0].strip() or not pair[1].strip():
                return "Query and topic cannot be empty", 400
        
        # Forward to localhost:8080 with self-identification header
        print(json.dumps(query_pairs))
        response = requests.post(
            'http://localhost:8080/update',
            data=json.dumps(query_pairs),
            headers={
                'Content-Type': 'application/json',
                'X-Client-Identity': 'http://127.0.0.1:8200/enqueue-data'
            }
        )
        
        # Return the response from the forwarded request
        return response.text, response.status_code
    
    except json.JSONDecodeError:
        return "Invalid JSON data", 400
    except requests.exceptions.RequestException as e:
        return f"Error forwarding query: {str(e)}", 500


@app.route('/all-streams', methods=['POST'])
def all_streams():   
    try:
        # Forward to localhost:8080 with self-identification header
        response = requests.post(
            'http://localhost:8080/update',
            data='SELECT * FROM employees',
            headers={
                'Content-Type': 'text/plain',
                'X-Client-Identity': 'http://127.0.0.1:8200/enqueue-data'
            }
        )
        return response.text, response.status_code
    except requests.exceptions.RequestException as e:
        return f"Error forwarding query: {str(e)}", 500


# Endpoint for other services to send data
@app.route('/enqueue-data', methods=['POST'])
def enqueue_data():
    data = request.json  # Expecting JSON data
    print(data)
    if data:
        data_queue.put(data)
        return "Data received and queued.", 200
    return "No data provided.", 400

# SSE endpoint for frontend to receive data
# SSE endpoint for frontend to receive data
@app.route('/stream-data')
def stream_data():
    print("New SSE client connected")  # Debug connection
    def event_stream():
        while True:
            try:
                data = data_queue.get_nowait()
                print(f"Sending data to client: {data}")  # Debug data sending
                # Ensure proper SSE format with double newlines
                yield f"data: {json.dumps(data)}\n\n"
            except Empty:
                print("Queue empty, waiting...")  # Debug empty queue
                time.sleep(0.5)
                
    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*'  # Explicit CORS for SSE
        }
    )

if __name__ == '__main__':
    app.run(port=8200, threaded=True)