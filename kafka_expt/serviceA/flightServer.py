import pyarrow as pa
import pyarrow.flight as flight
import json
from IOThreadpool import readThreadTask

class FlightServer(flight.FlightServerBase):
    def __init__(self, location, systemInfo, topicInfoDict, consumerInfo):
        super().__init__(location)
        self.systemInfo = systemInfo
        self.topicInfoDict = topicInfoDict
        self.consumerInfo = consumerInfo

    def do_get(self, context, ticket):
        request = ticket.ticket.decode('utf-8')
        request = json.loads(request)
        topic = request["topic"]
        offset = request["offset"]

        # Use the generator to stream Arrow tables
        generator = readThreadTask(self.systemInfo.dataPath, self.topicInfoDict, topic, offset)

        try:
            # Get the first table to initialize the stream
            first_table = next(generator)
        except StopIteration:
            raise flight.FlightInternalError("No data available at the requested offset.")

        # Create a generator to yield all record batches across all tables
        def batch_generator():
            for table in [first_table, *generator]:
                for batch in table.to_batches():
                    yield batch

        return flight.RecordBatchStream(pa.schema(first_table.schema), batch_generator())
