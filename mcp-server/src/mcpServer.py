from pyarrow import flight
from mcp.server.fastmcp import FastMCP
from mcpUtils import mcpUtils
import argparse

serviceAURL="grpc://localhost:8815"
serviceBURL="ws://localhost:8767/ws/querEndpoint"
serviceAlocation = flight.Location.for_grpc_tcp("localhost", 8815)


MCPServer=FastMCP("Composable architecture for fast analyticd")
MCPServerUtils=mcpUtils(serviceAlocation)


@MCPServer.tool(name="list_streams",description="This tool is used to list all the streams on the broker")
def list_streams() -> str:
    try:
        flights = MCPServerUtils.flightClient.list_flights()
        # Decode bytes in descriptor paths
        stream_list = [[p.decode() for p in f.descriptor.path] for f in flights]
        formatted_streams = [" / ".join(path) for path in stream_list]
        return "Available Streams:\n" + "\n".join(formatted_streams)
    except Exception as e:
        return f"Error listing streams: {e}"

@MCPServer.tool(name="fetch_stream_schema",description="this tool is used to fetch the schema of a stream")
def fetch_stream(topic:str=""):
    try:
        if not topic:
            return "Please provide a valid stream topic."
        descriptor = flight.FlightDescriptor.for_path(topic)
        schema_result=MCPServerUtils.flightClient.get_schema(descriptor)
        schema = schema_result.schema
        return f"Schema for topic '{topic}':\n{schema}\n there is another event_time (timestamp datatype) field which isnt visible in the schema"
    except Exception as e:
        return f"Error fetching schema for topic '{topic}': {e}"
    

    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Start MCP tool server with Flight client.")
    parser.add_argument("--token",type=str,default="",help="auth token for RBAC")
    
    args=parser.parse_args()
    MCPServerUtils.updateToken(args.token)
    MCPServer.run("stdio")