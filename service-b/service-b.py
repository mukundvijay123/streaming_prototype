import pyarrow as pa
import pyarrow.flight as flight
from flightServer import FlightServer
import threading
from clientUtils import subscribe,unsubscribe


if __name__ == "__main__":
    server = FlightServer()
    server_thread = threading.Thread(target=server.serve, daemon=True)
    server_thread.start()

    remoteAddress="grpc://localhost:8815"
    serverAddress="grpc://localhost:8816"
    subscribe("ABC",remoteAddress,serverAddress)
    subscribe("LMN",remoteAddress,serverAddress)
    try:
        while True:
            pass  # keep the server alive
    except KeyboardInterrupt:
        print("Shutting down server...")
        server.shutdown()
