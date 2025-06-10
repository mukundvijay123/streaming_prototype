package serviceb.arrowio;

import java.util.concurrent.BlockingQueue;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class Stream {
    private final BlockingQueue<VectorSchemaRoot> topicQueue;
    //public final String streamName; Not needed for now , will add later 
    public final String topic;
    public final Schema streamSchema;
    public final long pollIntervalMills;
   

    public Stream(String topic,Schema streamSchema,long pollInterval,BlockingQueue<VectorSchemaRoot> topicQueue){
        this.topic=topic;
        this.streamSchema=streamSchema;
        this.pollIntervalMills=pollInterval;
        this.topicQueue=topicQueue;
    }

    public BlockingQueue<VectorSchemaRoot> getQueue(){
        return this.topicQueue;
    }

}
