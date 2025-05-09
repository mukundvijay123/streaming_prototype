from QueryContext import queryContext
from readerwriterlock import rwlock
from fastapi import WebSocket
import janus
import clientUtils
import threading
import json
import pyarrow as pa
from pyarrow.lib import tobytes
import pyarrow.substrait as substrait
import utils



class systemQueryMetadata:
    def __init__(self, brokerAddress: str, myAddress: str,QueryPlanServerAddress:str):
        # Maps and shared resources
        self.QueryMap = {}      # sessionName -> queryContext
        self.TopicMap = {}      # topic -> set(queryContext)
        self.brokerAddress = brokerAddress
        self.myAddress = myAddress
        self.QueryPlanServerAddress=QueryPlanServerAddress
        self.__queryCount = 0
        self.outboundQueue = janus.Queue()

        # Reader/Writer lock
        self._lock = rwlock.RWLockFair()
        self.ReadQueryLock = self._lock.gen_rlock()
        self.WriteQueryLock = self._lock.gen_wlock()

    def createQueryName(self) -> str:
        with self.WriteQueryLock:
            self.__queryCount += 1
            return f"QuerySession{self.__queryCount}"

    def getQueryCtx(self, sessionName: str):
        with self.ReadQueryLock:
            return self.QueryMap.get(sessionName)

    def readTopicSubscribers(self, topic: str):
        with self.ReadQueryLock:
            return set(self.TopicMap.get(topic, []))

    def __subscriptionHandler(self, topic: str, subscribe: bool):
        # network I/O: subscribe or unsubscribe
        if subscribe:
            clientUtils.subscribe(topic, self.brokerAddress, self.myAddress)
        else:
            clientUtils.unsubscribe(topic, self.brokerAddress, self.myAddress)



    def createQuerySession(
        self,
        queryString: str,
        wsConn: WebSocket,
        test: bool = False,
        queryPlan: dict = None
    ) -> str:
        print("Triggered")
        plan = queryPlan 
        topic = utils.find_topics(plan)[0]
        print(topic)
        
        # 2) Generate session name and context
        sessionName = self.createQueryName()
        ctx = queryContext(sessionName, plan, self.outboundQueue, topic, wsConn)

        # 3) Register under lock and detect if first subscriber
        need_subscribe = False
        with self.WriteQueryLock:
            self.QueryMap[sessionName] = ctx
            subs = self.TopicMap.setdefault(topic, set())
            if not subs:
                need_subscribe = True
            subs.add(ctx)

        # 4) Perform subscription network I/O outside lock
        if need_subscribe:
            self.__subscriptionHandler(topic, True)

        # 5) Start the query’s execution thread
        ctx.start()
        return sessionName



    def deleteQuerySession(self, sessionName: str) -> bool:
        # 1) Remove under lock and detect if last subscriber
        need_unsubscribe = False
        with self.WriteQueryLock:
            ctx = self.QueryMap.pop(sessionName, None)
            if not ctx:
                return False

            topic = ctx.topic
            subs = self.TopicMap.get(topic, set())
            subs.discard(ctx)
            if not subs:
                # last one — clean up
                del self.TopicMap[topic]
                need_unsubscribe = True

        # 2) Unsubscribe outside lock
        if need_unsubscribe:
            self.__subscriptionHandler(topic, False)

        # 3) Stop the query context’s thread
        ctx.stop()
        return True
