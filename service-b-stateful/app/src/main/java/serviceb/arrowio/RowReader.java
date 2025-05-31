/*
 * THIS PACKAGE IS NOT THREAD SAFE
 */

package serviceb.arrowio;


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
    private static final String eventtimeColumnName="event_time";
    private final arrowIO source;
    private Instant watermark;
    private VectorSchemaRoot currentEventTable;
    private Instant timeStamp;
    private Instant LatestTime=Instant.ofEpochSecond(0);//add this logic
    private Iterator<Row> tableIterator;
    private Schema arrowSchema;
    private org.apache.beam.sdk.schemas.Schema beamSchema;
    private Row currentRow;
    private static final Integer maximumAllowedDelay=3;

    //what watermark to start from
    public RowReader(arrowIO source,Instant watermark){
        this.source=source;
        this.watermark=watermark;
        this.source.createConn();
        this.arrowSchema=source.arrowSchema;
        try{
            this.beamSchema=arrowIOUtils.ArrowSchemaConverter(this.arrowSchema);
        }catch(Exception e){
            e.printStackTrace();
        }
        
    }

    @Override
    public boolean start() throws IOException{
        boolean result=advance();
        //System.out.println("Create Result: " + result+"\n\n\n");
        return result;
    }


    @Override
    public boolean advance() throws IOException {
        try {
            // First, try to get the next row from current iterator
            if (this.tableIterator != null && this.tableIterator.hasNext()) {
                this.currentRow = this.tableIterator.next();
                //System.out.println("Advance Result: " + true+"\n\n\n");
                return true;
            }

            // If no more rows in current iterator, try to get new data
            VectorSchemaRoot temp = source.getEventTable();
            
            if (temp == null) {
                //System.out.println("Advance Result: " + false+"\n\n\n");
                return false;
            }

            
            // Process the new data
            Schema arrowSchema = temp.getSchema();
            Map<String, String> schemaMetadata = arrowSchema.getCustomMetadata();
            String timestamp = schemaMetadata.get("timestamp");
            Instant eventTime = new Instant(Long.parseLong(timestamp));
            this.timeStamp = eventTime;
            
            if (this.timeStamp.isAfter(this.LatestTime)) {
                this.LatestTime = this.timeStamp;
            }
            
            this.currentEventTable = temp;
            Iterable<Row> rows = () -> ArrowConversion.rowsFromRecordBatch(this.beamSchema, this.currentEventTable);
            this.tableIterator = rows.iterator();
            
            // Get the first row from the new iterator
            if (this.tableIterator.hasNext()) {
                try{
                    this.currentRow = this.tableIterator.next();
                    //System.out.println(currentRow);
                }catch(Exception e){
                    //System.out.println("This aint good , idk how to fix it ");
                }
                
                //System.out.println("Advance Result: " + true+"\n\n\n");
                return true;
            }
            
            //System.out.println("Advance Result: " + false+"\n\n\n");
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.getMessage(), e);
        }
    }



    @Override
    public Row getCurrent() throws NoSuchElementException {
        if (currentRow == null) {
            throw new NoSuchElementException("No current row available");
        }
        //adding event time in the pcollection row
        Row.Builder builder = Row.withSchema(this.beamSchema);
        for (int i = 0; i < this.beamSchema.getFieldCount(); i++) {
            String fieldName = this.beamSchema.getField(i).getName();
            if (!fieldName.equals(eventtimeColumnName)) {
                builder.addValue(currentRow.getValue(fieldName));
            }
        }


        builder.addValue(this.timeStamp);

        return builder.build();
    }



    public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (this.timeStamp == null) {
            return Instant.now(); // Fallback timestamp
        }
        return this.timeStamp;
    }

    @Override
    public Instant getWatermark(){
        //this logic has to be fixed
        this.watermark = this.LatestTime.minus(Duration.standardSeconds(maximumAllowedDelay));
        
        //System.out.println("getWaterMark Result"+this.watermark+"\n\n\n");
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
        //System.out.println("GetCurrentSource called"+"\n\n\n");
        return this.source;
    }



    

}
