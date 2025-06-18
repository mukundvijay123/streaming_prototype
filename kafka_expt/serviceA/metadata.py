import json
import os
import threading


#Class for system metaqdata
class systemMetadata:
    def __init__(self):
        self.maxMessageSize=1048576
        self.maxSegmentSize=10485760
        self.topics=set()
        self.writeThreads=3
        self.dataPath=r"partitions"

    #Method that reads system_metadata.json and updates
    def  readfile(self):
        systemMetadataFilePath=r'metadata\system_metadata.json'
        fp=open(systemMetadataFilePath,'r')
        systemInfo=json.load(fp)
        fp.close()
        self.maxSegmentSize=systemInfo["maxSegmentSize"]
        self.maxMessageSize=systemInfo["maxMessageSize"]
        self.writeThreads=systemInfo["NoOfWriteThreads"]
        self.dataPath=systemInfo["dataPath"]
        for topic in systemInfo["topicNames"]:
            self.topics.add(topic)

    #Method that prints contents
    def __str__(self):
        return (
            f"System Metadata:\n"
            f"  Max Message Size: {self.maxMessageSize}\n"
            f"  Max Segment Size: {self.maxSegmentSize}\n"
            f"  Write Threads: {self.writeThreads}\n"
            f"  Topics: {', '.join(self.topics) if self.topics else 'None'}"
        )
    
    #method for JSON serialization
    def __repr__(self):
        return (
            f"maxSegmentSize:{self.maxSegmentSize}, "
            f"maxMessageSize:{self.maxMessageSize}, "
            f"writeThreads:{self.writeThreads}, "
            f"topics:{list(self.topics)},"
            f"dataPath:{self.dataPath}"
        )








#Class for topic metadata
class topicMetadata:
    def __init__(self,topic,offset,lastSegment,segments):
        self.topic=topic  #topic name 
        self.offset=offset #last commited offset
        self.segments=segments #segment numbers list datatype
        self.lastSegment=lastSegment #name of current segment
        self.fileWriteLock=threading.Lock()
        self.updateObjectLock=threading.Lock()
        self.SegmentLogSize=0
        self.SegmentLog=None #file descripto for .log file
        self.SegmentIndex=None #file descriptor for .index file
    
    def __str__(self):
        return (
            f"Topic Metadata:\n"
            f"  Topic: {self.topic}\n"
            f"  Offset: {self.offset}\n"
            f"  Last Segment: {self.lastSegment}\n"
            f"  Segments: {self.segments}"
        )

    def __repr__(self):
        return (
            f"topic:{self.topic}, "
            f"offset:{self.offset}, "
            f"segments:{self.segments}, "
            f"lastSegment:{self.lastSegment}"
        )




#Consumer information class
class consumerMetadata:
    def __init__(self,id):
        self.consumerId=id #unique id of the consumer
        self.topicsSubcribed=set() #topics subscribes by the consumer
        self.offsets={} #last consumed offsets





#Function to create topicMetadataDict
def createTopicMetadataDict(systemMetadataInfo):
    metadataJsonFilepath=r'metadata\topic_metadata.json'
    dataPath=systemMetadataInfo.dataPath
    fp=open(metadataJsonFilepath,'r')
    topicInfo=json.load(fp)
    fp.close()
    topicMetadataDict={}
    for topic in topicInfo.keys():
        topicMetadataDict[topic]=topicMetadata(topic,topicInfo[topic]['offset'],topicInfo[topic]['lastSegment'],topicInfo[topic]['segments'])
        lastSegment=topicInfo[topic]['lastSegment']
        if lastSegment is not None:
            topicMetadataDict[topic].SegmentLog=open(os.path.join(segmentPath,f'{lastSegment}.log'),"ab")
            topicMetadataDict[topic].SegmentIndex=open(os.path.join(segmentPath,f'{lastSegment}.index'),"ab")

        else :
            newSegment=0
            topicMetadataDict[topic].lastSegment = newSegment
            topicMetadataDict[topic].segments.append(newSegment)

            segmentPath = os.path.join(dataPath, topic)           


            topicMetadataDict[topic].SegmentLog=open(os.path.join(segmentPath,f'{newSegment}.log'),"ab")
            topicMetadataDict[topic].SegmentIndex=open(os.path.join(segmentPath,f'{newSegment}.index'),"ab")

    return topicMetadataDict