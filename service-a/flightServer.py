import pyarrow as pa
import pyarrow.flight as flight
from utils import is_valid_grpc_address,extract_subscription
from metadata import systemMetadata


class FlightServer(flight.FlightServerBase):
    def __init__(self,systemMetadata:systemMetadata,location="grpc://0.0.0.0:8815"):
        super().__init__(location)
        self._location=location
        self.systemMetadata=systemMetadata

    
    def list_actions(self,context):
        return[
            ("subscribe","subscribe to the stream"),
            ("unsubscribe","unsubscribe to the stream"),
        ]
    
    def do_action(self,context,action):
        print(action.type)
        success=None
        if action.type=="subscribe":
            print("hello")
            address,topic=extract_subscription(action)
            if is_valid_grpc_address(address) and self.systemMetadata.hasTopic(topic):
                print("hi")
                success=self.systemMetadata.addConsumer(topic ,address)
                response_msg="Success"
        elif action.type=="unsubscribe":
            address,topic=extract_subscription(action)
            if is_valid_grpc_address(address) and self.systemMetadata.hasTopic(topic):
                success=self.systemMetadata.removeConsumer(topic ,address)
                response_msg=f"Success"
        else:
            raise NotImplementedError
        print(self.systemMetadata)
        if success:
            response_bytes=response_msg.encode('utf-8')
        else :
            response_bytes="error".encode('utf-8')
        return iter([flight.Result(response_bytes)])
            

            

