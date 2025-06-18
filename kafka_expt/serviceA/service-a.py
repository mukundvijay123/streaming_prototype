import pyarrow as pa
from streamSimulator import streamSimulator
from metadata import systemMetadata, createTopicMetadataDict
from IOThreadpool import writerThreadTask
from queue import Queue
import threading
from flightServer import FlightServer
import pyarrow.flight as flight

eventQueue = Queue()

if __name__ == "__main__":
    systemInfo = systemMetadata()
    systemInfo.readfile()
    print(systemInfo)
    topicInfo = createTopicMetadataDict(systemInfo)
    print(topicInfo)

    simulatorThread = threading.Thread(target=streamSimulator, args=(eventQueue,))
    simulatorThread.daemon = True
    simulatorThread.start()

    threads = []
    for i in range(systemInfo.writeThreads):
        t = threading.Thread(target=writerThreadTask, args=(eventQueue, topicInfo,))
        t.start()
        threads.append(t)

    # Start Flight server
    location = flight.Location.for_grpc_tcp("0.0.0.0", 8815)
    server = FlightServer(location, systemInfo, topicInfo, consumerInfo={})
    print("Starting Flight server on port 8815...")
    server.serve()  # This blocks, so it should be the last thing called

    for t in threads:
        t.join()
