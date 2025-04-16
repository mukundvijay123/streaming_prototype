from readerwriterlock import rwlock

class systemMetadata:
    def __init__(self):
        self.topics = set()
        self.num_topics = 0
        self.consumers = {}  # key: topic, value: set of WebSocket connections

        self.topic_lock = rwlock.RWLockFairD()
        self.topic_read_lock = self.topic_lock.gen_rlock()
        self.topic_write_lock = self.topic_lock.gen_wlock()

        self.consumer_lock = rwlock.RWLockFairD()
        self.consumer_read_lock = self.consumer_lock.gen_rlock()
        self.consumer_write_lock = self.consumer_lock.gen_wlock()

    def addTopic(self, topic):
        with self.topic_write_lock:
            self.topics.add(topic)
            self.num_topics += 1
        with self.consumer_write_lock:
            self.consumers[topic] = set()

    def removeTopic(self, topic):
        with self.topic_write_lock:
            if topic in self.topics:
                self.topics.remove(topic)
                self.num_topics -= 1
        with self.consumer_write_lock:
            self.consumers.pop(topic, None)

    def hasTopic(self, topic):
        with self.topic_read_lock:
            return topic in self.topics

    def readTopics(self):
        with self.topic_read_lock:
            return list(self.topics)

    def addConsumer(self, topic, ws_conn):
        with self.topic_read_lock:
            if topic not in self.topics:
                return
        with self.consumer_write_lock:
            self.consumers[topic].add(ws_conn)

    def removeConsumer(self, topic, ws_conn):
        with self.topic_read_lock:
            if topic not in self.topics:
                return
        with self.consumer_write_lock:
            self.consumers[topic].discard(ws_conn)

    def getSubscribers(self, topic):
        with self.consumer_read_lock:
            # Return an empty list if the topic isn't registered.
            return list(self.consumers.get(topic, []))

    def __str__(self):
        with self.topic_read_lock, self.consumer_read_lock:
            return (
                f"systemMetadata(topics={list(self.topics)}, "
                f"num_topics={self.num_topics}, "
                f"consumers={self.consumers})"
            )
