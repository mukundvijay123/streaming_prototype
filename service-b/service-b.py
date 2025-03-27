import pyarrow as pa
from FlightServer import FlightServer
from Client import subscribe,unsubscribe,subscribe_test
from queue import Queue
import multiprocessing
from time import sleep




def startFlightServer(EventQueue):
    FlightServerAddress='grpc://127.0.0.1:8816'
    server=FlightServer(EventQueue,location=FlightServerAddress)
    print(f"Starting FlightServer at {FlightServerAddress}...")
    server.serve()

def event_consumer(EventQueue):
    while True:
        event =EventQueue.get()
        print("Received Event",event)

if __name__=="__main__":
    EventQueue= multiprocessing.Queue()
    FlightServerAddress='grpc://127.0.0.1:8816' #This is service-B server's address
    RemoteAddress='grpc://127.0.0.1:8815'#This is service-B server's address
    
    #starting Flight Server
    server_process = multiprocessing.Process(target=startFlightServer,args=(EventQueue,), daemon=True)
    server_process.start()
    sleep(2)
    #sends do action request to serviceA from serviceB client
    #subscribe_test is used here , this has to be converted to original subscribe in client.py
    subscribe_test(RemoteAddress,FlightServerAddress)
    #This is just a test , dequeues and prints events
    event_consumer(EventQueue)
    