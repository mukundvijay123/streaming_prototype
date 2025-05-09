from flask import Flask, request, jsonify, render_template_string
import requests
import json

app = Flask(__name__)

# Configuration
SQL_SERVICE_URL = "http://127.0.0.1:8080"

# HTML template for the test UI
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>SQL Service Test Client</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { display: flex; }
        .panel { flex: 1; margin: 10px; padding: 15px; border: 1px solid #ccc; border-radius: 5px; }
        textarea { width: 100%; height: 150px; margin-bottom: 10px; }
        button { padding: 8px 15px; background-color: #4CAF50; color: white; border: none; cursor: pointer; }
        .output { margin-top: 20px; padding: 10px; background-color: #f5f5f5; border-radius: 5px; white-space: pre-wrap; }
        h2 { color: #333; }
    </style>
</head>
<body>
    <h1>SQL Service Test Client</h1>
    
    <div class="container">
        <div class="panel">
            <h2>Create Table Definition</h2>
            <div>
                <label for="create-topic">Topic:</label>
                <input type="text" id="create-topic" placeholder="e.g., employees">
            </div>
            <div>
                <label for="create-statement">CREATE TABLE Statement:</label>
                <textarea id="create-statement" placeholder="CREATE TABLE employees (id INT NOT NULL, name VARCHAR(100), salary INT NOT NULL)"></textarea>
            </div>
            <button onclick="sendRequest('/create')">Create</button>
            <div class="output" id="create-output"></div>
        </div>
        
        <div class="panel">
            <h2>Alter Table Definition</h2>
            <div>
                <label for="alter-topic">Topic:</label>
                <input type="text" id="alter-topic" placeholder="e.g., employees">
            </div>
            <div>
                <label for="alter-statement">Updated CREATE TABLE Statement:</label>
                <textarea id="alter-statement" placeholder="CREATE TABLE employees (id INT NOT NULL, name VARCHAR(100), salary INT NOT NULL, department VARCHAR(50))"></textarea>
            </div>
            <button onclick="sendRequest('/alter')">Alter</button>
            <div class="output" id="alter-output"></div>
        </div>
    </div>
    
    <div class="container">
        <div class="panel">
            <h2>Get Substrait Plan</h2>
            <div>
                <label for="query">SQL Query:</label>
                <textarea id="query" placeholder="SELECT id, name FROM employees WHERE salary > 50000"></textarea>
            </div>
            <button onclick="sendRequest('/getSubstrait')">Get Plan</button>
            <div class="output" id="substrait-output"></div>
        </div>
        
        <div class="panel">
            <h2>Delete Table Definition</h2>
            <div>
                <label for="delete-topic">Topic:</label>
                <input type="text" id="delete-topic" placeholder="e.g., employees">
            </div>
            <button onclick="sendRequest('/delete')">Delete</button>
            <div class="output" id="delete-output"></div>
        </div>
    </div>
    
    <script>
        function sendRequest(endpoint) {
            let url = '/test' + endpoint;
            let data = {};
            let outputId = '';
            
            if (endpoint === '/create') {
                data = {
                    topic: document.getElementById('create-topic').value,
                    createTableStatement: document.getElementById('create-statement').value
                };
                outputId = 'create-output';
            } else if (endpoint === '/alter') {
                data = {
                    topic: document.getElementById('alter-topic').value,
                    createTableStatement: document.getElementById('alter-statement').value
                };
                outputId = 'alter-output';
            } else if (endpoint === '/getSubstrait') {
                data = {
                    query: document.getElementById('query').value
                };
                outputId = 'substrait-output';
            } else if (endpoint === '/delete') {
                data = {
                    topic: document.getElementById('delete-topic').value
                };
                outputId = 'delete-output';
            }
            
            // Fixed: Send requests to the Flask server instead of directly to the SQL service
            fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            })
            .then(response => response.json())
            .then(data => {
                document.getElementById(outputId).textContent = JSON.stringify(data, null, 2);
            })
            .catch(error => {
                document.getElementById(outputId).textContent = 'Error: ' + error;
            });
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Render the test UI"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/test/create', methods=['POST'])
def test_create():
    """Test the /create endpoint"""
    data = request.json
    try:
        response = requests.post(
            f"{SQL_SERVICE_URL}/create", 
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        print("Response is:",response)
        return jsonify({
            'status': response.status_code,
            'content': response.json() if response.headers.get('content-type') == 'application/json' else response.text
        })
    except Exception as e:
        return jsonify({
            'status': 500,
            'error': str(e)
        })

@app.route('/test/alter', methods=['POST'])
def test_alter():
    """Test the /alter endpoint"""
    data = request.json
    try:
        response = requests.post(
            f"{SQL_SERVICE_URL}/alter", 
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        return jsonify({
            'status': response.status_code,
            'content': response.json() if response.headers.get('content-type') == 'application/json' else response.text
        })
    except Exception as e:
        return jsonify({
            'status': 500,
            'error': str(e)
        })

@app.route('/test/getSubstrait', methods=['POST'])
def test_get_substrait():
    """Test the /getSubstrait endpoint"""
    data = request.json
    print(data)
    try:
        response = requests.post(
            f"{SQL_SERVICE_URL}/getSubstrait", 
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        return jsonify({
            'status': response.status_code,
            'content': response.json() if response.headers.get('content-type') == 'application/json' else response.text
        })
    except Exception as e:
        return jsonify({
            'status': 500,
            'error': str(e)
        })

@app.route('/test/delete', methods=['POST'])
def test_delete():
    """Test the /delete endpoint"""
    data = request.json
    try:
        response = requests.post(
            f"{SQL_SERVICE_URL}/delete", 
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        return jsonify({
            'status': response.status_code,
            'content': response.json() if response.headers.get('content-type') == 'application/json' else response.text
        })
    except Exception as e:
        return jsonify({
            'status': 500,
            'error': str(e)
        })

if __name__ == '__main__':
    print("Flask test client for SQL Service started at http://localhost:5000")
    print("Make sure your Java SQL Service is running at", SQL_SERVICE_URL)
    app.run(debug=True)