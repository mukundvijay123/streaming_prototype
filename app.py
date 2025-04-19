from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/', methods=['POST'])  # Explicitly only allow POST
def handle_post():
    try:
        data = request.get_json()
        print(data)
        if not data:
            return jsonify({"error": "No JSON data received"}), 400
            
        print("\nReceived data:")
        print(f"Client IP: {data.get('clientAddress')}")
        print(f"Query: {data.get('query')}")
        print("Plan:", data.get('plan'))
        
        return jsonify({"status": "success"}), 200
        
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(port=6789, debug=True)