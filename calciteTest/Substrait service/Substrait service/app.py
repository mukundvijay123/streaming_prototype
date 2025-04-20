import requests
import json

# Path to the JSON file containing the Substrait plan
json_file_path = "substrait_plan.json"

# Read the JSON plan from the file
try:
    with open(json_file_path, "r") as json_file:
        substrait_plan_json = json.load(json_file)
        #print("Loaded Substrait plan:", substrait_plan_json)
except FileNotFoundError:
    print(f"Error: {json_file_path} not found.")
    exit(1)
except json.JSONDecodeError:
    print(f"Error: Failed to decode JSON from {json_file_path}.")
    exit(1)

# Define the FastAPI endpoint
url = "http://localhost:8000/run-substrait"
url2 = "http://localhost:8000/client-plans"

# Send the JSON plan to the FastAPI service
response = requests.post(url, json=substrait_plan_json)
response2 = requests.get(url2)
#client -> calcite -> python service
# Check if the request was successful
if response.status_code == 200:
    # Print the results received from the server
    print("Response from server:", response.json())
    print("Clients: ", response2.json())
else:
    print(f"Failed to execute Substrait plan. Status code: {response.status_code}")

