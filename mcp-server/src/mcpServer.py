from pyarrow import flight
from mcp.server.fastmcp import FastMCP
from mcpUtils import mcpUtils
import argparse
import os
from mcpStreamer import start_websocket_app
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
    
@MCPServer.tool(name="getStreamingData",
            description="""Use the fetch_stream_schema to get the schema and then come to this tool.
            This tool is used to fetch streaming data based on what the user has asked.
            Convert what the user has asked into an SQL query and pass that query in the function in order to use this function. 
            Use this if user wants data of stream, but convert what the user wants into a full fledged SQL query. 
            NOTE: before calling this tool, fetch the schema for the topic requested and then frame the query.
            The topic argument is a list of strings(topicnames)"""
            )
def get_streaming_data(query:str, topic:list):
    try:
        start_websocket_app(query, topic, MCPServerUtils.token)
    except Exception as e:
        return f"""There was an error while creating the query session{e}."""

    return "Query Session created successfully"

    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Start MCP tool server with Flight client.")
    parser.add_argument("--token",type=str,default="",help="auth token for RBAC")
    
    args=parser.parse_args()
    MCPServerUtils.updateToken(args.token)
    MCPServer.run("stdio")
