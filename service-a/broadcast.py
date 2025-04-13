import threading
from queue import Queue
import pyarrow as pa
import pyarrow.flight as flight

class FlightBroadcaster:
    def __init__(self):
        self.task_queue = Queue()
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()

    def broadcast(self, subscriber_set, events, topic):
        # Push a new broadcast task into the queue
        self.task_queue.put((subscriber_set, events, topic))
        

    def _worker(self):
        while True:
            subscriber_set, events, topic = self.task_queue.get()
            #print(events[0])
            if not events:
                continue
            for subscriber in subscriber_set:
                try:
                    client = flight.connect(subscriber)
                    descriptor = flight.FlightDescriptor.for_path(f"stream/{topic}")
                    writer, _ = client.do_put(descriptor, events[0].schema)
                    for event in events:
                        if isinstance(event, pa.Table):
                            writer.write_table(event)
                    writer.close()
                except Exception as e:
                    print(f"Failed to send to {subscriber}: {e}")
