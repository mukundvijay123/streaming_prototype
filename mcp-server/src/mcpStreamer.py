import asyncio
import json
import logging
import threading
import queue
from datetime import datetime
from typing import  Tuple
import time
import sys
import os
from WebsocketClient import WebSocketClient
from MessageDisplayApp import MessageDisplayApp


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

# Thread management functions
def run_gui_thread(message_queue: queue.Queue):
    """Function to run GUI in a separate thread"""
    try:
        app = MessageDisplayApp(message_queue,logger)
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
        
        client = WebSocketClient(message_queue,logger)
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