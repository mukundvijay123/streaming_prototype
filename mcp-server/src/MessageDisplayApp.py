import tkinter as tk
from tkinter import scrolledtext, ttk
import queue
from datetime import datetime
import logging
import json



class MessageDisplayApp:
    """Simple Tkinter app to display WebSocket messages"""
    
    def __init__(self, message_queue: queue.Queue,logger:logging.Logger):
        self.message_queue = message_queue
        self.message_count = 0
        self.is_running = True
        self.logger=logger
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
            self.logger.error(f"Error checking messages: {e}")
        
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
