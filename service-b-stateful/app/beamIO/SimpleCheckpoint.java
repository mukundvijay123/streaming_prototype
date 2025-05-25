package beam.streaming.arrowio;

import java.io.Serializable;

import org.apache.beam.sdk.io.UnboundedSource;

class SimpleCheckpoint implements UnboundedSource.CheckpointMark, Serializable {
    public SimpleCheckpoint(){

    }

    @Override
    public void finalizeCheckpoint() {
        
    }
}
