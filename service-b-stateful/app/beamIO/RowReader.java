package beam.streaming.arrowio;


import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.joda.time.Instant;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;;

class RowReader  extends UnboundedReader<Row> {
    private final ArrowIO source;
    private Instant watermark;
    private VectorSchemaRoot currentEventTable;
    private Instant timeStamp;
    private Instant LatestTime=Instant.ofEpochSecond(0);//add this logic
    private Iterator<Row> tableIterator;
    private final Schema arrowSchema;
    private org.apache.beam.sdk.schemas.Schema beamSchema;
    private static final Integer maximumAllowedDelay=300;

    //what watermark to start from
    public RowReader(ArrowIO source,Instant watermark,Schema schema){
        this.source=source;
        this.watermark=watermark;
        this.timeStamp=null;
        this.currentEventTable=null;
        this.tableIterator=null;
        this.arrowSchema=schema;
        this.beamSchema=ArrowConversion.ArrowSchemaTranslator.toBeamSchema(this.arrowSchema.getFields());
    }

    @Override
    public boolean start() throws IOException{
        return advance();
    }

    @Override
    public boolean advance()throws IOException{
        try{
            if(this.tableIterator==null||!this.tableIterator.hasNext()){
                VectorSchemaRoot temp=source.getEventTable();
                if(temp==null){
                    return false;
                }
                Schema arrowSchema = temp.getSchema();
                Map<String, String> schemaMetadata = arrowSchema.getCustomMetadata();
                String timestamp=schemaMetadata.get("timestamp");
                Instant eventTime= new Instant(Long.parseLong(timestamp));
                this.timeStamp=eventTime;
                if(this.timeStamp.isAfter(this.LatestTime)){
                    this.LatestTime=this.timeStamp;
                }
                this.currentEventTable=temp;
                Iterable<Row> rows=()->ArrowConversion.rowsFromRecordBatch(this.beamSchema, this.currentEventTable);
                this.tableIterator=rows.iterator();
                return true;
            }
            return false;
        }catch(Exception e){
            throw new IOException("Interrupted while reading from queue", e);
        }
    }

    @Override
    public Row getCurrent()throws NoSuchElementException{
        try{
            Row currentRow=tableIterator.next();
            return currentRow;
        }catch(Exception e){
            throw new NoSuchElementException("No new rows exist but pipeline tried to access");
        }
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        try {
            if (!advance()) {
                throw new NoSuchElementException("No element exists");
            }
            return this.timeStamp;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get current timestamp", e);
        }
    }

    @Override
    public Instant getWatermark(){
        //this logic has to be fixed
        this.watermark = this.LatestTime.minus(Duration.standardSeconds(maximumAllowedDelay));
        return this.watermark;
    }

    @Override
        public SimpleCheckpoint getCheckpointMark(){
            return new SimpleCheckpoint();
        }

    @Override
    public void close(){
        //Nothing to do
    }

    @Override
    public UnboundedSource<Row,?>getCurrentSource(){
        return this.source;
    }



    

}
