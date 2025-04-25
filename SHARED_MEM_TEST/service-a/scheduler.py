import threading 
from time import sleep 
from metadata import systemMetadata
from queueMap import QueueMap
from broadcast import FlightBroadcaster

class Scheduler:
    def __init__(self,systemMetadata:systemMetadata,queueMap:QueueMap,eventsPerFlight:int):
        self.BroadcastThreads=[]  #channels is just the name there are queues in this list
        self.schedulerThread =threading.Thread(target=self._schedule,daemon=True)
        self.systemMetadata=systemMetadata
        self.eventsPerFlight=eventsPerFlight
        self.queueMap=queueMap 

        


    def _schedule(self):
        while True:
            for worker in self.BroadcastThreads:
                topics=self.systemMetadata.readTopics()
                for topic in topics:
                    subscribers=self.systemMetadata.getSubscribers(topic)
                    events=self.queueMap.getEvents(topic,self.eventsPerFlight)
                    if events[0]==None:
                        continue
                    worker.broadcast(subscribers,events,topic)

            sleep(0.01) #put to avoid continuous polling , dont know the behaviour of the thread


    def AddBroadcastThread(self, broadcaster:FlightBroadcaster):
        self.BroadcastThreads.append(broadcaster)

    def start(self):
        self.schedulerThread.start()

    def __str__(self):
        return (f"Scheduler("
                f"BroadcastThreads={len(self.BroadcastThreads)}, "
                f"eventsPerFlight={self.eventsPerFlight}, "
                f"systemMetadata={self.systemMetadata}, "
                f"queueMap={self.queueMap})")




