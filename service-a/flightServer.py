import pyarrow as pa 
import pyarrow.flight as flight
import json

def fetchSchema(conn):
    cursor=conn.cursor()
    cursor.execute("SELECT * FROM stock_prices_2 LIMIT 1")
    arrow_schema = cursor.fetch_schema()
    return arrow_schema


def extract_address(action):
    try:
        data = json.loads(action.body.to_pybytes().decode("utf-8"))
        if "address" not in data:
            raise ValueError("Action body does not contain 'address' field.")
        return data["address"]
    except Exception as e:
        raise ValueError(f"Failed to extract address: {e}")

    

class FlightServer(flight.FlightServerBase):
    def __init__(self,subscriber_set,conn,location="grpc://0.0.0.0:8815"):
        super().__init__(location)
        self._location=location
        self.conn=conn
        self.subscribers=subscriber_set

    
    def _make_flight_info(self,topic):
        schema=fetchSchema(self.conn)
        descriptor=flight.FlightDescriptor.for_path(topic)
        endpoints=[flight.FlightEndpoint(topic,self.location)]
        return flight.FlightInfo(schema,descriptor,endpoints)

    def list_flights(self,context,criteria):
        #Put names of all the topic youu want to stream here
        #In this prototype there is only one topic
        topics=["XYZ"]
        for topic in topics:
            yield self._make_flight_info(topic)

    def list_actions(self, context):
        return[
            ("subscribe","subscribe to the stream"),
            ("unsubscribe","unsubscribe to the stream"),
            ]

    def do_action(self,context,action):
        print("action")
        if action.type=="subscribe":
            address=extract_address(action) 
            self.subscribers.add(address)
            print(self.subscribers)
            response_msg=f"Succesfully subscribed address:{address}"
        elif action.type=="unsubscribe":
            address=extract_address(action)
            self.subscribers.remove(address)
            print(self.subscribers)
            response_msg=f"Succesfully subscribed address:{address}"
        else:
            raise NotImplementedError
        
        response_bytes=response_msg.encode('utf-8')
        return iter([flight.Result(response_bytes)])
        

        


    

    




