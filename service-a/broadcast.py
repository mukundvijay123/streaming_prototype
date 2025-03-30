import pyarrow as pa
import pyarrow.flight as flight


#This is a client which sends data to subscribers
def broadcast(subscriber_set, queue):
    while True:
        event = queue.get()  # Retrieve the PyArrow Table from the queue
        
        # Validate that the event is a PyArrow Table
        if not isinstance(event, pa.Table):
            print("Invalid event format. Expected a PyArrow Table.")
            continue

        for subscriber in subscriber_set:
            try:
                # Establish a Flight connection to the subscriber
                flight_client = flight.connect(subscriber)

                # Define the flight descriptor (stream name)
                
                descriptor = flight.FlightDescriptor.for_path("stock_stream")

                # Open a Flight writer
                writer, _ = flight_client.do_put(descriptor, event.schema)

                # Send the entire table at once
                writer.write_table(event)  
                # Close the writer to signal completion
                writer.close()

            except Exception as e:
                print(f"Failed to send data to {subscriber}: {e}")
