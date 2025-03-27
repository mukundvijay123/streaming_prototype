import pyarrow  as pa
import pyarrow.flight as flight

class FlightServer(flight.FlightServerBase):
    def __init__(self,queue,location="grpc://127.0.0.1:8815"):
        super(FlightServer,self).__init__(location)
        self.location=location
        self.EventQueue=queue


    def do_put(self,context,descriptor,reader,writer):
        event_descriptor=descriptor.path[0].decode('utf-8')
        full_table=reader.read_all()
        event_dict=full_table.to_pydict()
        print(event_dict)
        self.EventQueue.put(event_dict)
        print(f"Added full table to queue for event{event_descriptor}")

