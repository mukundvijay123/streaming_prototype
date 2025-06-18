from queue import Queue

class QueueMap:
    def __init__(self , topics):
        self.queueMap={}
        for topic in topics:
            self.queueMap[topic]=Queue()

    
