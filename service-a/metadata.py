from readerwriterlock import rwlock

class systemMetadata:
    def __init__(self, broadcastThreads):
        self.topics = set()
        self.num_topics = 0
        self.broadcastThreads = broadcastThreads
        self.consumers = {}  # key: topic, value: set of gRPC addresses

        # Locks
        self.topic_lock = rwlock.RWLockFairD()
        self.consumer_lock = rwlock.RWLockFairD()

        # Read and write locks
        self.topic_read_lock = self.topic_lock.gen_rlock()
        self.topic_write_lock = self.topic_lock.gen_wlock()

        self.consumer_read_lock = self.consumer_lock.gen_rlock()
        self.consumer_write_lock = self.consumer_lock.gen_wlock()

    def addTopic(self, topic):
        # Always acquire topic lock before consumer lock
        with self.topic_write_lock:
            self.topics.add(topic)
            self.num_topics += 1

            # Now safely add to consumers dict
            with self.consumer_write_lock:
                self.consumers[topic] = set()

    def removeTopic(self, topic):
        # Always acquire topic lock before consumer lock
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

    def addConsumer(self, topic, grpc_address):
        # First check if topic exists under topic read lock
        with self.topic_read_lock:
            if topic not in self.topics:
                return False

        # Then add consumer under consumer write lock
        with self.consumer_write_lock:
            self.consumers.setdefault(topic, set()).add(grpc_address)
            return True

    def removeConsumer(self, topic, grpc_address):
        # Check topic existence first
        with self.topic_read_lock:
            if topic not in self.topics:
                return False

        # Then modify consumers safely
        with self.consumer_write_lock:
            self.consumers.get(topic, set()).discard(grpc_address)
            return True

    def getSubscribers(self, topic):
        with self.consumer_read_lock:
            return list(self.consumers.get(topic, set()))

    def __str__(self):
        # Always acquire topic lock before consumer lock
        with self.topic_read_lock:
            with self.consumer_read_lock:
                return (f"systemMetadata(topics={list(self.topics)}, "
                        f"num_topics={self.num_topics}, "
                        f"broadcastThreads={self.broadcastThreads}, "
                        f"consumers={self.consumers})")
