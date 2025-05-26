package serviceb.arrowio;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
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

public class arrowIO extends UnboundedSource<Row, SimpleCheckpoint>{
    public static final StreamRegistry Registry =new StreamRegistry();
    public transient BlockingQueue<VectorSchemaRoot> InputQueue;
    public transient Schema arrowSchema;
    private transient Stream stream;
    private final String topic;
    private final String streamName;
    private final String ContextName; //this is session name
    private Instant watermark;

    public arrowIO(String topic,String streamName, String contextName,Instant watermark){
        this.topic=topic;
        this.streamName=streamName;
        this.ContextName=contextName;
        this.watermark=watermark;
    }

    public void createConn(){
        //Injecting non serializable attributes into the arrowIO class
       try{
        Stream stream=Registry.getStream(this.ContextName,this.streamName);
        this.stream=stream;
        this.arrowSchema=stream.streamSchema;
        this.InputQueue=stream.getQueue();
       }catch(Exception e){
        e.printStackTrace();
       }
    }

    public void putEventTable(VectorSchemaRoot eventTable){
        if(this.InputQueue==null){
            System.out.println("Input Queue is null\n");
        }
        this.InputQueue.add(eventTable);
    }

    public VectorSchemaRoot getEventTable() {
        if (InputQueue == null) {
            System.out.println("Input Queue is null\n");
            return null; // early return if queue is null
        }
        try {
            return this.InputQueue.poll(this.stream.pollIntervalMills,TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            Thread.currentThread().interrupt(); // preserve interrupt status
            System.err.println("Interrupted while polling the queue: " + e.getMessage());
            return null;
        }
    }




    @Override
    public List<? extends UnboundedSource<Row, SimpleCheckpoint>> split(
            int desiredNumSplits, PipelineOptions options) {
        // for simplicity, don't split -> no splitting
        return List.of(this);
    }

    @Override
    public UnboundedReader<Row> createReader(PipelineOptions options,SimpleCheckpoint checkpointMark){
        return new RowReader(this, this.watermark);
    }

    @Override
    public Coder<SimpleCheckpoint> getCheckpointMarkCoder(){
        return SerializableCoder.of(SimpleCheckpoint.class);
    }

    @Override
    public Coder<Row> getOutputCoder() {
        this.createConn();
        org.apache.beam.sdk.schemas.Schema beamRowSchema=ArrowConversion.ArrowSchemaTranslator.toBeamSchema(arrowSchema.getFields());
        return org.apache.beam.sdk.coders.RowCoder.of(beamRowSchema);
    }

}
