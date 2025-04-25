from queueMap import QueueMap
from streamSimulator import streamSimulator
from broadcast import FlightBroadcaster
from metadata import systemMetadata
from scheduler import Scheduler
from flightServer import FlightServer
import threading

system_metadata=systemMetadata(3)
system_metadata.addTopic("ABC")
system_metadata.addTopic("XYZ")
system_metadata.addTopic("LMN")
#print(system_metadata)

queue_map=QueueMap()
queue_map.add_topic("ABC")
queue_map.add_topic("XYZ")
queue_map.add_topic("LMN")

scheduler=Scheduler(system_metadata,queue_map,5)
#print(scheduler)

           
for _ in range(system_metadata.broadcastThreads):
    broadcastThread=FlightBroadcaster()
    scheduler.AddBroadcastThread(broadcastThread)
#print(scheduler)

streamSimulatorThread=threading.Thread(target=streamSimulator,args=(queue_map,))
streamSimulatorThread.start()

scheduler.start()

server=FlightServer(system_metadata)
server.serve()
