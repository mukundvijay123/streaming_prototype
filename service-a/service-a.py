import pyarrow as pa
import pyarrow.flight as flight
from queue import Queue
from streamSimulator import streamSimulator,setup
from flightServer import FlightServer
from broadcast import broadcast
import threading

EventQueue=Queue()
SubscriberSet=set()
conn=setup()
server=FlightServer(SubscriberSet,conn)

if __name__=="__main__":
    streamThread=threading.Thread(target=streamSimulator,args=(EventQueue,))
    broadcastThread=threading.Thread(target=broadcast,args=(SubscriberSet,EventQueue))
    streamThread.start()
    broadcastThread.start()
    server.serve()
