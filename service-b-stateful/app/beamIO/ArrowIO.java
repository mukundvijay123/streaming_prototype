package beam.streaming.arrowio;

import java.util.concurrent.BlockingQueue;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

public class ArrowIO extends UnboundedSource<Row, SimpleCheckpoint>{
    private  BlockingQueue<VectorSchemaRoot> InputQueue;
    private final String topic;
    private Schema arrowSchema;
    private Instant watermark;

    public ArrowIO(String topic,
    BlockingQueue<VectorSchemaRoot> InputQueue,Schema schema,Instant watermark){
        this.topic=topic;
        this.InputQueue=InputQueue;
        this.arrowSchema=schema;
        this.watermark=watermark;
    }

    public void putEventTable(VectorSchemaRoot eventTable){
        this.InputQueue.add(eventTable);
    }

    public VectorSchemaRoot getEventTable(){
        return this.InputQueue.poll();
    }

    @Override
    public List<? extends UnboundedSource<Row, SimpleCheckpoint>> split(
            int desiredNumSplits, PipelineOptions options) {
        // for simplicity, don't split -> no splitting
        return List.of(this);
    }

    @Override
    public UnboundedReader<Row> createReader(PipelineOptions options,SimpleCheckpoint checkpointMark){
        return new RowReader(this, this.watermark, this.arrowSchema);
    }

    @Override
    public Coder<SimpleCheckpoint> getCheckpointMarkCoder(){
        return SerializableCoder.of(SimpleCheckpoint.class);
    }

    @Override
    public Coder<Row> getOutputCoder() {
        org.apache.beam.sdk.schemas.Schema beamRowSchema=ArrowConversion.ArrowSchemaTranslator.toBeamSchema(this.arrowSchema.getFields());
        return org.apache.beam.sdk.coders.RowCoder.of(beamRowSchema);
    }




    


}
