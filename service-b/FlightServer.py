import pyarrow as pa
import pyarrow.flight as flight



class FlightServer(flight.FlightServerBase):
    def __init__(self, location="grpc://0.0.0.0:8816"):
        super().__init__(location)

    def do_put(self, context, descriptor, reader, writer):
        print(f"\nReceiving stream for: {descriptor.path}")
        try:
            batch_number = 0
            while True:
                try:
                    chunk = reader.read_chunk()
                    if chunk is None:
                        break

                    record_batch = chunk.data
                    table = pa.Table.from_batches([record_batch])
                    print(f"\nBatch {batch_number}:")
                    print(table)
                    batch_number += 1

                except StopIteration:
                    break

        except Exception as e:
            print("Error while reading table:", e)