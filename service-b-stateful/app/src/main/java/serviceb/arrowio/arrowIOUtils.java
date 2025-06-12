/*
 * THIS PACKAGE IS NOT THREAD SAFE
*/
package serviceb.arrowio;

import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Date;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;




public class arrowIOUtils  {
    public arrowIOUtils(){
        
    }

    public static Schema ArrowSchemaConverter(org.apache.arrow.vector.types.pojo.Schema InputArrowSchema)throws Exception{
        Schema temp=ArrowConversion.ArrowSchemaTranslator.toBeamSchema(InputArrowSchema);
        if(temp.hasField("event_time")){
            throw new Exception("This name is reserved in the schema");
        }
        Schema.Builder builder=Schema.builder();

        for(Schema.Field field:temp.getFields()){
            builder.addField(field);
        }

        builder.addDateTimeField("event_time");
        return builder.build();
    }
    //This method ensures all time relate fields in a row are in org.joda.Instant
    public static Row CustomRowBuilder(Schema schema, Row row,String eventtimeColumnName,Instant eventTime){
        Row.Builder builder = Row.withSchema(schema);
        for (Schema.Field field : schema.getFields()){
            String fieldName = field.getName();
            Object value;
            if (eventtimeColumnName.equals(fieldName)) {
                // Use the supplied eventTime for this field
                value = eventTime;
            }else{
                Object raw = row.getValue(fieldName);
                value = convertToInstantIfApplicable(raw);
            }
            builder.addValue(value);

        }

        return builder.build();
    }
    private static Object convertToInstantIfApplicable(Object value) {
        if (value == null) return null;

        if (value instanceof Instant) {
            return value;
        } else if (value instanceof org.joda.time.DateTime) {
            return ((org.joda.time.DateTime) value).toInstant();
        } else if (value instanceof java.time.Instant) {
            return new Instant(((java.time.Instant) value).toEpochMilli());
        } else if (value instanceof java.time.LocalDateTime) {
            java.time.LocalDateTime ldt = (java.time.LocalDateTime) value;
            return new Instant(ldt.toInstant(ZoneOffset.UTC).toEpochMilli());
        } else if (value instanceof java.util.Date) {
            return new Instant(((Date) value).getTime());
        } else if (value instanceof Temporal) {
            return new Instant(java.time.Instant.from((Temporal) value).toEpochMilli());
        }else if(value instanceof java.lang.Integer || value instanceof java.lang.Long){
            long epochSeconds = value instanceof Integer
                ? ((Integer) value).longValue()
                : (Long) value;
            return new Instant(epochSeconds * 1000L);
        }else{
            System.out.println(value);
            System.out.println(value.getClass().getName());
        }

    return value;
}



}
