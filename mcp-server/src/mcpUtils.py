import pyarrow.flight as flight


class mcpUtils:
    def __init__(self,location):
        self.flightClient:flight.FlightClient=flight.FlightClient(location)
        self.token:str=None

    def updateToken(self,token:str)->None:
        self.token=token
    
        