from queue import Queue
from readerwriterlock import rwlock

class QueueMap():
    def __init__(self):
        self.queueMap={}
        self.lock = rwlock.RWLockFairD()  # Fair read/write lock
        self.read_lock = self.lock.gen_rlock()
        self.write_lock = self.lock.gen_wlock()

    def add_topic(self,topic):
        with self.write_lock:
            if self.queueMap.get(topic) ==None :
                self.queueMap[topic]=Queue()
            
    def del_topic(self,topic):
        with self.write_lock:
            if self.queueMap.get(topic)!=None:
                del self.queueMap[topic]

    def getEvents(self,topic,num):
        events=[]
        with self.read_lock:
            queue =self.queueMap.get(topic)
            if queue is None:
                return [None]
            for _ in range(num):
                try:
                    events.append(queue.get_nowait())
                except :
                    events.append(None)
                    break
        return events
    

    def putEvent(self,topic,event):
        with self.read_lock:
            queue=self.queueMap.get(topic)
            if queue is not None:
                queue.put(event)
                return True #succesfull enqueue
            else:
                return False #enqueue failed
            
    


