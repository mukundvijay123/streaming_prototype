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
    query = request.data.decode('utf-8')
    print(query)
    if not query:
        return "No query provided.", 400
    
    try:
        # Forward to localhost:8080 with self-identification header
        response = requests.post(
            'http://localhost:8080/update',
            data=query,
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