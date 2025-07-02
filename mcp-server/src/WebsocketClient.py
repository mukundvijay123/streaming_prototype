import asyncio
import json
import logging
import threading
import tkinter as tk
from tkinter import scrolledtext, ttk
import queue
from datetime import datetime
from typing import Optional, Tuple
import time
import sys
import os
import websockets  # You'll need to install this: pip install websockets

# Fix encoding issues for Windows
if sys.platform.startswith('win'):
    # Set environment variable for UTF-8 support
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    
    # Try to set console to UTF-8 mode
    try:
        import locale
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    except:
        pass

# Configure logging with UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class MessageDisplayApp:
    """Simple Tkinter app to display WebSocket messages"""
    
    def __init__(self, message_queue: queue.Queue):
        self.message_queue = message_queue
        self.message_count = 0
        self.is_running = True
        
        # Create the main window
        self.root = tk.Tk()
        self.root.title("WebSocket Message Monitor")
        self.root.geometry("900x650")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        
        self.setup_gui()
        
        # Start checking for messages
        self.check_messages()
    
    def setup_gui(self):
        """Set up the GUI components"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(main_frame, text="WebSocket Messages", 
                               font=("Arial", 14, "bold"))
        title_label.pack(pady=(0, 10))
        
        # Status frame
        status_frame = ttk.Frame(main_frame)
        status_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Message counter
        self.count_var = tk.StringVar(value="Messages: 0")
        count_label = ttk.Label(status_frame, textvariable=self.count_var)
        count_label.pack(side=tk.LEFT)
        
        # Clear button
        clear_btn = ttk.Button(status_frame, text="Clear Messages", 
                              command=self.clear_messages)
        clear_btn.pack(side=tk.RIGHT)
        
        # Messages display area
        self.messages_text = scrolledtext.ScrolledText(
            main_frame,
            width=100,
            height=35,
            wrap=tk.WORD,
            font=("Consolas", 9),
            bg="black",
            fg="white"
        )
        self.messages_text.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        # Configure text tags for different message types
        self.messages_text.tag_config("timestamp", foreground="#888888")
        self.messages_text.tag_config("sent", foreground="#66B2FF")      # Light blue
        self.messages_text.tag_config("received", foreground="#66FF66")   # Light green
        self.messages_text.tag_config("error", foreground="#FF6666")      # Light red
        self.messages_text.tag_config("connection", foreground="#FFAA66") # Orange
        
    def add_message(self, message: str, msg_type: str = "INFO"):
        """Add a message to the display"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]  # Include milliseconds
        
        # Insert timestamp
        self.messages_text.insert(tk.END, f"[{timestamp}] ", "timestamp")
        
        # Insert message with appropriate formatting (using simple text instead of emojis)
        if msg_type == "SENT":
            self.messages_text.insert(tk.END, ">> SENT: ", "sent")
            tag = "sent"
        elif msg_type == "RECEIVED":
            self.messages_text.insert(tk.END, "<< RECEIVED: ", "received")
            tag = "received"
        elif msg_type == "ERROR":
            self.messages_text.insert(tk.END, "!! ERROR: ", "error")
            tag = "error"
        elif msg_type == "CONNECTION":
            self.messages_text.insert(tk.END, "-- CONNECTION: ", "connection")
            tag = "connection"
        else:
            self.messages_text.insert(tk.END, f"** {msg_type}: ")
            tag = "received"
        
        # Insert the actual message
        self.messages_text.insert(tk.END, f"{message}\n", tag)
        
        # Try to pretty-print JSON
        try:
            if message.strip().startswith('{') or message.strip().startswith('['):
                data = json.loads(message)
                formatted_json = json.dumps(data, indent=2)
                self.messages_text.insert(tk.END, f"   FORMATTED:\n{formatted_json}\n", tag)
        except json.JSONDecodeError:
            pass
        
        # Add separator
        self.messages_text.insert(tk.END, "-" * 80 + "\n\n")
        
        # Auto-scroll to bottom
        self.messages_text.see(tk.END)
        
        # Update counter
        self.message_count += 1
        self.count_var.set(f"Messages: {self.message_count}")
    
    def clear_messages(self):
        """Clear all messages from display"""
        self.messages_text.delete(1.0, tk.END)
        self.message_count = 0
        self.count_var.set("Messages: 0")
    
    def check_messages(self):
        """Check for new messages in the queue"""
        if not self.is_running:
            return
            
        try:
            # Check for messages without blocking
            messages_processed = 0
            while messages_processed < 50:  # Limit to prevent GUI freezing
                try:
                    message_data = self.message_queue.get_nowait()
                    self.add_message(message_data['message'], message_data['type'])
                    messages_processed += 1
                except queue.Empty:
                    break
        except Exception as e:
            logger.error(f"Error checking messages: {e}")
        
        # Schedule next check
        if self.is_running:
            self.root.after(50, self.check_messages)  # Check every 50ms for better responsiveness
    
    def on_close(self):
        """Handle window close event"""
        self.is_running = False
        self.root.quit()
        self.root.destroy()
    
    def run(self):
        """Start the GUI application"""
        self.root.mainloop()


class WebSocketClient:
    """Modified WebSocket client that sends messages to GUI via queue"""
    
    def __init__(self, message_queue: queue.Queue):
        self.websocket = None
        self.is_connected = False
        self.is_running = False
        self.message_queue = message_queue
    
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
            logger.debug(f"Message queued: {msg_type} - {message[:100]}...")
        except queue.Full:
            logger.warning("Message queue is full, dropping message")
        except Exception as e:
            logger.error(f"Error sending message to GUI: {e}")
    
    async def connect_to_server(self, uri: str = "ws://localhost:8767/ws/queryEndpoint"):
        """Connect to the WebSocket server"""
        try:
            self.websocket = await websockets.connect(uri)
            self.is_connected = True
            self.send_to_gui(f"Connected to {uri}", "CONNECTION")
            logger.info(f"Connected to WebSocket server: {uri}")
            return True
        except Exception as e:
            error_msg = f"Failed to connect to WebSocket server: {e}"
            self.send_to_gui(error_msg, "ERROR")
            logger.error(error_msg)
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
            logger.info(f"Sending message structure: action={msg['action']}, topics type={type(msg['topics'])}, topics={msg['topics']}")
            
            json_message = json.dumps(msg, ensure_ascii=False)
            await self.websocket.send(json_message)
            self.send_to_gui(json.dumps(msg, indent=2), "SENT")
            logger.info(f"Sent query request successfully")
            return True
        except Exception as e:
            error_msg = f"Failed to send query request: {e}"
            self.send_to_gui(error_msg, "ERROR")
            logger.error(error_msg)
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
                logger.info(f"Received WebSocket message: {message[:200]}...")
                self.send_to_gui(message, "RECEIVED")
                
                # Add a small delay to prevent overwhelming the GUI
                await asyncio.sleep(0.01)
                
        except websockets.exceptions.ConnectionClosed as e:
            self.send_to_gui(f"WebSocket connection closed: {e}", "CONNECTION")
            logger.info(f"WebSocket connection closed: {e}")
        except Exception as e:
            if self.is_running:  # Only log if we weren't intentionally disconnecting
                error_msg = f"Error while listening: {e}"
                self.send_to_gui(error_msg, "ERROR")
                logger.error(error_msg)
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
            logger.info("Disconnected from WebSocket server")
    
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
            logger.error(f"Client error: {e}")


# Thread management functions
def run_gui_thread(message_queue: queue.Queue):
    """Function to run GUI in a separate thread"""
    try:
        app = MessageDisplayApp(message_queue)
        app.run()
    except Exception as e:
        logger.error(f"GUI thread error: {e}")


def run_websocket_thread(message_queue: queue.Queue, query: str, topics, token: str, uri: str):
    """Function to run WebSocket client in a separate thread"""
    try:
        # Ensure topics is a list
        if topics is None:
            topics = []
        elif isinstance(topics, str):
            topics = [topics] if topics.strip() else []
        elif not isinstance(topics, list):
            topics = list(topics) if hasattr(topics, '__iter__') else [str(topics)]
        
        # Create new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        client = WebSocketClient(message_queue)
        loop.run_until_complete(client.run_client(query, topics, token, uri))
    except Exception as e:
        logger.error(f"WebSocket thread error: {e}")
    finally:
        loop.close()


def spawn_threads(query: str, topics, token: str, uri: str = "ws://localhost:8767/ws/queryEndpoint") -> Tuple[threading.Thread, threading.Thread, queue.Queue]:
    """
    Spawn GUI and WebSocket client threads
    
    Args:
        query: Query string to send
        topics: List of topics (will be converted to list if needed)
        token: Authentication token
        uri: WebSocket URI (optional)
    
    Returns:
        Tuple containing (gui_thread, websocket_thread, message_queue)
    """
    # Ensure topics is a list
    if topics is None:
        topics = []
    elif isinstance(topics, str):
        topics = [topics] if topics.strip() else []
    elif not isinstance(topics, list):
        topics = list(topics) if hasattr(topics, '__iter__') else [str(topics)]
    
    # Create message queue for communication between threads
    message_queue = queue.Queue(maxsize=1000)
    
    # Create GUI thread
    gui_thread = threading.Thread(
        target=run_gui_thread,
        args=(message_queue,),
        name="GUI-Thread",
        daemon=True  # Dies when main thread dies
    )
    
    # Create WebSocket thread  
    websocket_thread = threading.Thread(
        target=run_websocket_thread,
        args=(message_queue, query, topics, token, uri),
        name="WebSocket-Thread",
        daemon=True  # Dies when main thread dies
    )
    
    return gui_thread, websocket_thread, message_queue


def safe_print(text: str):
    """Safely print text, handling encoding issues"""
    try:
        print(text)
    except UnicodeEncodeError:
        # Fall back to ASCII representation
        print(text.encode('ascii', 'replace').decode('ascii'))


def start_websocket_app(query: str, topics, token: str, uri: str = "ws://localhost:8767/ws/queryEndpoint"):
    """
    Convenience function to start the complete WebSocket application
    
    Args:
        query: Query string to send
        topics: List of topics (can be list, string, or None)
        token: Authentication token
        uri: WebSocket URI (optional)
    
    Returns:
        Tuple containing (gui_thread, websocket_thread, message_queue)
    """
    # Ensure topics is properly formatted as a list
    if topics is None:
        topics = []
    elif isinstance(topics, str):
        topics = [topics] if topics.strip() else []
    elif not isinstance(topics, list):
        topics = list(topics) if hasattr(topics, '__iter__') else [str(topics)]
    
    safe_print("Starting WebSocket Application...")
    safe_print(f"Query: {query}")
    safe_print(f"Topics: {topics} (type: {type(topics)})")
    safe_print(f"URI: {uri}")
    safe_print("-" * 50)
    
    # Spawn threads
    gui_thread, websocket_thread, message_queue = spawn_threads(query, topics, token, uri)
    
    # Start threads
    gui_thread.start()
    safe_print(f"[OK] GUI thread started: {gui_thread.name} (ID: {gui_thread.ident})")
    
    # Wait a moment for GUI to initialize
    time.sleep(1)
    
    websocket_thread.start()
    safe_print(f"[OK] WebSocket thread started: {websocket_thread.name} (ID: {websocket_thread.ident})")
    
    return gui_thread, websocket_thread, message_queue


# Example usage
if __name__ == "__main__":
    # Example parameters
    query = "test query"
    topics = ["topic1", "topic2"]
    token = "your_token_here"
    
    # Start the application
    gui_thread, ws_thread, msg_queue = start_websocket_app(query, topics, token)
    
    try:
        # Keep main thread alive
        gui_thread.join()
    except KeyboardInterrupt:
        safe_print("Application terminated by user")