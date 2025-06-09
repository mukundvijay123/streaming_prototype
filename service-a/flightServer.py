import pyarrow as pa
import pyarrow.flight as flight
from utils import is_valid_grpc_address,extract_subscription
from metadata import systemMetadata
import adbc_driver_postgresql.dbapi as adbc


DB_URI="postgresql://postgres:123456789@localhost:5432/arrow_kafka"

class FlightServer(flight.FlightServerBase):
    def __init__(self,systemMetadata:systemMetadata,location="grpc://0.0.0.0:8815"):
        super().__init__(location)
        self._location=location
        self.systemMetadata=systemMetadata
        self.adbcConn=adbc.connect(uri=DB_URI)

    def __GetStreamSchema(self,topic):
        cursor=self.adbcConn.cursor()
        query=("SELECT * FROM stock_prices_2 WHERE stock_symbol = $1;")
        cursor.execute(query,(topic,))
        data=cursor.fetch_arrow_table()
        return data.schema

    def get_schema(self, context, descriptor):
        # Expecting the topic to be passed as a path descriptor
        if descriptor.descriptor_type == flight.DescriptorType.PATH:
            # Get topic from path
            topic = descriptor.path[0]
            
            # Get the Arrow Schema from the source (e.g., PostgreSQL via ADBC)
            schema = self.__GetStreamSchema(topic)
            
            # Return as a SchemaResult
            return flight.SchemaResult(schema)
        
        raise flight.FlightServerError("Unsupported descriptor type or missing topic")

    
    def list_actions(self,context):
        return[
            ("subscribe","subscribe to the stream"),
            ("unsubscribe","unsubscribe to the stream"),
        ]
    
    def do_action(self,context,action):
        success=None
        if action.type=="subscribe":
            address,topic=extract_subscription(action)
            if is_valid_grpc_address(address) and self.systemMetadata.hasTopic(topic):
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
            

            

