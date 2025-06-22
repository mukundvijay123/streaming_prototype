from queue import Queue
import threading
import pyarrow as pa
from pyarrow.lib import tobytes
import pyarrow.substrait as substrait
import json
from fastapi import WebSocket
import janus

class queryContext:
    def __init__(self,QueryName:str,QueryPlan:str,outboundQueue:janus.Queue,topic:str,wsConn:WebSocket, token:str=None):
        self.QueryName=QueryName
        self.inboundQueue=Queue()
        self.QueryPlan=QueryPlan
        self.topic=topic
        self.outboundQueue=outboundQueue
        self.__QueryThread=threading.Thread(target=self.__runQuery,daemon=True)
        self.__stopEvent=threading.Event()
        self.__wsConn=wsConn
        self.token = token  # Store the user's JWT or auth context

    def start(self):
        plan_bytes = tobytes(json.dumps(self.QueryPlan))
        buf = pa._substrait._parse_json_plan(plan_bytes)
        self.QueryPlan=buf

        self.__QueryThread.start()
        #self.__QueryThread.join()

    def stop(self):
        self.__stopEvent.set()

    
    def __eventProvider(self,streams,schema):
        if streams[0]==self.topic:
            return self.inboundQueue.get()
            
    def __runQuery(self):
        while not self.__stopEvent.is_set():
            reader = pa.substrait.run_query(self.QueryPlan, table_provider=self.__eventProvider)
            event=reader.read_all()
            print(event)
            if isinstance(event,pa.Table):
                metadata=event.schema.metadata or {}
                metadata[b'queryContext']=self.QueryName.encode()
                event=event.replace_schema_metadata(metadata)
                self.outboundQueue.sync_q.put(event)
    

    def addEvent(self,event):
        self.inboundQueue.put(event)

    async def sendEvent(self,eventString:str):
        await self.__wsConn.send_text(eventString)



    def __hash__(self):
        return hash(self.QueryName)
    
    def __eq__(self, other):
        return isinstance(other, queryContext) and self.QueryName == other.QueryName





