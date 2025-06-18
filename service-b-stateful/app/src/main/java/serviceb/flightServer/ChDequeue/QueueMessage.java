package serviceb.flightServer.ChDequeue;

import org.apache.arrow.vector.VectorSchemaRoot;

public class QueueMessage {
    private final String topic;
    private final VectorSchemaRoot arrowData;

    public QueueMessage(String topic, VectorSchemaRoot arrowData) {
            this.topic = topic;
            this.arrowData = arrowData;
    }

    public String getTopic() {
        return topic;
    }

    public VectorSchemaRoot getArrowData() {
        return arrowData;
    }

    @Override
    public String toString() {
        return "QueueMessage{" +
                "topic='" + topic + '\'' +
                ", data=" + (arrowData != null ? arrowData : 0) +
                '}';
    }    
}
