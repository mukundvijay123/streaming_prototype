/*
 * THIS PACKAGE IS NOT THREAD SAFE
 */

package serviceb.arrowio;


import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.joda.time.Instant;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.*;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;;
import org.apache.arrow.vector.ValueVector;


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
       // System.out.println("Arrow schema is: " + arrowSchema.toString());
        Field timestampField = new Field("eventtime",
                new FieldType(true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), null),
                null);

        this.arrowSchema = new Schema(Arrays.asList(
                timestampField,
                new Field("stock_symbol", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("price", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("volume", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("bid_price", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("ask_price", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("spread", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
 //       System.out.println("Arrow schema is: " + arrowSchema.toString());
        try{
            this.beamSchema=arrowIOUtils.ArrowSchemaConverter(this.arrowSchema);
      //      System.out.println("Beam schema is: " + beamSchema.toString());
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


    public boolean advance() throws IOException {
        try {
            // First, try to get the next row from current iterator
            if (this.tableIterator != null && this.tableIterator.hasNext()) {
                Row originalRow = this.tableIterator.next();
                this.currentRow = originalRow;
                return true;
            }

            // If no more rows in current iterator, try to get new data
            VectorSchemaRoot temp = source.getEventTable();
            if (temp == null) {
                return false;
            }

            // Process the new data
            Schema arrowSchema = temp.getSchema();
            Map<String, String> schemaMetadata = arrowSchema.getCustomMetadata();
            String timestamp = schemaMetadata.get("timestamp");
            System.out.println(timestamp);

            Instant eventTime = new Instant(Long.parseLong(timestamp) * 1000);
            System.out.println(eventTime);
            this.timeStamp = eventTime;

            if (this.timeStamp.isAfter(this.LatestTime)) {
                this.LatestTime = this.timeStamp;
            }

            this.currentEventTable = temp;

            Iterable<Row> rows = () -> ArrowConversion.rowsFromRecordBatch(this.beamSchema, this.currentEventTable);
            this.tableIterator = rows.iterator();

            // Get the first row from the new iterator
            if (this.tableIterator.hasNext()) {
                try {
                    Row originalRow = this.tableIterator.next();
                    this.currentRow = originalRow;
                } catch (Exception e) {
                    System.out.println("This aint good , idk how to fix it ");
                }
                return true;
            }

            return false;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.getMessage(), e);
        }
    }

    private Row castTimestampToInstant(Row originalRow) {
        // Get the schema to understand field types and positions
        org.apache.beam.sdk.schemas.Schema schema = originalRow.getSchema();

        // Create a new row with modified timestamp field
        List<Object> values = new ArrayList<>();

        for (int i = 0; i < schema.getFieldCount(); i++) {
            org.apache.beam.sdk.schemas.Schema.Field field = schema.getField(i);
            Object value = originalRow.getValue(i);

            // Check if this field is a timestamp field that needs conversion
            if (field.getName().toLowerCase().contains("timestamp") ||
                    field.getName().toLowerCase().contains("time")) {

                if (value instanceof Long) {
                    // Convert milliseconds to Instant
                    values.add(new Instant((Long) value));
                } else if (value instanceof String) {
                    // Parse string timestamp to Instant
                    try {
                        long timestampMs = Long.parseLong((String) value);
                        values.add(new Instant(timestampMs));
                    } catch (NumberFormatException e) {
                        // If parsing fails, keep original value
                        values.add(value);
                    }
                } else {
                    // Keep original value if it's not a recognizable timestamp format
                    values.add(value);
                }
            } else {
                // Keep original value for non-timestamp fields
                values.add(value);
            }
        }

        // Create new row with modified values
        return Row.withSchema(schema).addValues(values).build();
    }


    @Override
    public Row getCurrent() throws NoSuchElementException {
        if (currentRow == null) {
            throw new NoSuchElementException("No current row available");
        }
        System.out.println("get current");
//
//// Get the schema and field info
//        org.apache.beam.sdk.schemas.Schema schema = currentRow.getSchema();
//        org.apache.beam.sdk.schemas.Schema.Field timestampField = schema.getField("timestamp");
//        org.apache.beam.sdk.schemas.Schema.FieldType timestampType = timestampField.getType();
//
//        System.out.println("Field 'timestamp' type: " + timestampType);
//        System.out.println("Field 'timestamp' type kind: " + timestampType.getTypeName());
//        System.out.println("Field 'timestamp' nullable: " + timestampField.getType().getNullable());
//
//// Get the actual value and its Java type
//        Object timestampValue = currentRow.getValue("timestamp");
//        System.out.println("Timestamp value: " + timestampValue);
//        System.out.println("Timestamp value class: " + (timestampValue != null ? timestampValue.getClass().getName() : "null"));
//
//// Try different ways to access the timestamp
//        try {
//            System.out.println("currentRow.getDateTime('timestamp'): " + currentRow.getDateTime("timestamp"));
//        } catch (Exception e) {
//            System.out.println("Error calling getDateTime(): " + e.getMessage());
//        }
//
//        try {
//            System.out.println("currentRow.getLogicalTypeValue('timestamp', Instant.class): " +
//                    currentRow.getLogicalTypeValue("timestamp", org.joda.time.Instant.class));
//        } catch (Exception e) {
//            System.out.println("Error calling getLogicalTypeValue(): " + e.getMessage());
//        }
//
//// Check if it's a specific numeric type
//        if (timestampValue instanceof Number) {
//            Number numValue = (Number) timestampValue;
//            System.out.println("Numeric value: " + numValue.longValue());
//            System.out.println("As milliseconds: " + new org.joda.time.Instant(numValue.longValue()));
//            System.out.println("As microseconds: " + new org.joda.time.Instant(numValue.longValue() / 1000));
//        }
        Row row = arrowIOUtils.CustomRowBuilder(beamSchema, currentRow, eventtimeColumnName, this.timeStamp);
     //   System.out.println(row.getFieldCount());
        return row;
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

    public static void printVectorSchemaRoot(VectorSchemaRoot root) {
        int rowCount = root.getRowCount();
        List<FieldVector> fieldVectors = root.getFieldVectors();

        for (int i = 0; i < rowCount; i++) {
            StringBuilder rowStr = new StringBuilder("Row " + i + ": ");
            for (FieldVector vector : fieldVectors) {
                Object value = vector.getObject(i);
                rowStr.append(value).append(" | ");
            }
            System.out.println(rowStr.toString());
        }
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
