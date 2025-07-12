import asyncio
import json
import logging
import queue
from datetime import datetime
import logging
import websockets

class WebSocketClient:
    """Modified WebSocket client that sends messages to GUI via queue"""
    
    def __init__(self, message_queue: queue.Queue,logger:logging.Logger):
        self.websocket = None
        self.is_connected = False
        self.is_running = False
        self.message_queue = message_queue
        self.logger=logger
    
    def send_to_gui(self, message: str, msg_type: str = "INFO"):
        """Send message to GUI via queue"""
        try:
            message_data = {
                'message': str(message),
                'type': str(msg_type),
                'timestamp': datetime.now().isoformat()
            }
            # Try to put message in queue with timeout
            self.message_queue.put(message_data, block=True, timeout=1.0)
            self.logger.debug(f"Message queued: {msg_type} - {message[:100]}...")
        except queue.Full:
            self.logger.warning("Message queue is full, dropping message")
        except Exception as e:
            self.logger.error(f"Error sending message to GUI: {e}")
    
    async def connect_to_server(self, uri: str = "ws://localhost:8767/ws/queryEndpoint"):
        """Connect to the WebSocket server"""
        try:
            self.websocket = await websockets.connect(uri)
            self.is_connected = True
            self.send_to_gui(f"Connected to {uri}", "CONNECTION")
            self.logger.info(f"Connected to WebSocket server: {uri}")
            return True
        except Exception as e:
            error_msg = f"Failed to connect to WebSocket server: {e}"
            self.send_to_gui(error_msg, "ERROR")
            self.logger.error(error_msg)
            self.is_connected = False
            return False
    
    async def send_query_request(self, query_string: str, topics: list, token: str):
        """Send query request to server"""
        if not self.is_connected or not self.websocket:
            self.send_to_gui("Not connected to WebSocket server", "ERROR")
            return False
        
        try:
            # Ensure topics is always a list, even if empty
            if topics is None:
                topics = []
            elif isinstance(topics, str):
                # If topics is a string, convert to list
                topics = [topics] if topics.strip() else []
            elif not isinstance(topics, list):
                # Convert other types to list
                topics = list(topics) if hasattr(topics, '__iter__') else [str(topics)]
            
            msg = {
                "action": "start_query_session",
                "query_string": str(query_string),
                "topics": topics,  # This will be serialized as a JSON array
                "token": str(token)
            }
            
            # Debug logging to verify the structure
            self.logger.info(f"Sending message structure: action={msg['action']}, topics type={type(msg['topics'])}, topics={msg['topics']}")
            
            json_message = json.dumps(msg, ensure_ascii=False)
            await self.websocket.send(json_message)
            self.send_to_gui(json.dumps(msg, indent=2), "SENT")
            self.logger.info(f"Sent query request successfully")
            return True
        except Exception as e:
            error_msg = f"Failed to send query request: {e}"
            self.send_to_gui(error_msg, "ERROR")
            self.logger.error(error_msg)
            return False
    
    async def listen_for_messages(self):
        """Listen for messages from the server"""
        if not self.is_connected or not self.websocket:
            self.send_to_gui("Not connected to WebSocket server", "ERROR")
            return
        
        self.is_running = True
        self.send_to_gui("Started listening for messages...", "CONNECTION")
        
        try:
            async for message in self.websocket:
                if not self.is_running:
                    break
                
                # Log the received message
                self.logger.info(f"Received WebSocket message: {message[:200]}...")
                self.send_to_gui(message, "RECEIVED")
                
                # Add a small delay to prevent overwhelming the GUI
                await asyncio.sleep(0.01)
                
        except websockets.exceptions.ConnectionClosed as e:
            self.send_to_gui(f"WebSocket connection closed: {e}", "CONNECTION")
            self.logger.info(f"WebSocket connection closed: {e}")
        except Exception as e:
            if self.is_running:  # Only log if we weren't intentionally disconnecting
                error_msg = f"Error while listening: {e}"
                self.send_to_gui(error_msg, "ERROR")
                self.logger.error(error_msg)
        finally:
            self.is_running = False
            self.is_connected = False
            self.send_to_gui("Connection closed", "CONNECTION")
    
    async def disconnect(self):
        """Disconnect from the server"""
        self.is_running = False
        if self.websocket:
            await self.websocket.close()
            self.is_connected = False
            self.send_to_gui("Disconnected from WebSocket server", "CONNECTION")
            self.logger.info("Disconnected from WebSocket server")
    
    async def run_client(self, query: str, topics: list, token: str, uri: str = "ws://localhost:8767/ws/queryEndpoint"):
        """Main client execution"""
        try:
            # Connect to server
            if await self.connect_to_server(uri):
                # Send query request
                await self.send_query_request(query, topics, token)
                # Listen for messages
                await self.listen_for_messages()
        except Exception as e:
            self.send_to_gui(f"Client error: {e}", "ERROR")
            self.logger.error(f"Client error: {e}")
