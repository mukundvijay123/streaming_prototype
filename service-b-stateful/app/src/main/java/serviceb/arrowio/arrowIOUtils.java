/*
 * THIS PACKAGE IS NOT THREAD SAFE
*/
package serviceb.arrowio;

import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Date;

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
    public static Row CustomRowBuilder(Schema schema, Row row, String eventtimeColumnName, Instant eventTime) {
        Row.Builder builder = Row.withSchema(schema);



        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.getName();

            Object value;
            if (eventtimeColumnName.equals(fieldName)) {
                value = eventTime;
            } else {
                Object raw = row.getValue(fieldName);
                value = convertValueForField(raw, field);
            }

            builder.addValue(value);

        }
        Row result = builder.build();

        return result;


    }

    private static Object convertValueForField(Object value, Schema.Field field) {
        if (value == null) return null;

        Schema.FieldType fieldType = field.getType();

        // Handle different field types appropriately
        switch (fieldType.getTypeName()) {
            case INT32:
                return convertToInteger(value);
            case INT64:
                return convertToLong(value);
            case DOUBLE:
                return convertToDouble(value);
            case FLOAT:
                return convertToFloat(value);
            case STRING:
                return convertToString(value);
            case DATETIME:
                return convertToInstant(value);
            default:
                // For other types, try to return as-is or convert based on the value type
                return convertToInstantIfApplicable(value);
        }
    }

    private static Integer convertToInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Long) {
            Long longValue = (Long) value;
            // Check if the long value fits in an integer range
            if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                return longValue.intValue();
            } else {
                System.out.println("Warning: Long value " + longValue + " exceeds Integer range, using Long.intValue()");
                return longValue.intValue(); // This might cause overflow
            }
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                System.out.println("Cannot convert string to integer: " + value);
                return null;
            }
        }
        return null;
    }

    private static Long convertToLong(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                System.out.println("Cannot convert string to long: " + value);
                return null;
            }
        }
        return null;
    }

    private static Double convertToDouble(Object value) {
        if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Float) {
            return ((Float) value).doubleValue();
        } else if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                System.out.println("Cannot convert string to double: " + value);
                return null;
            }
        } else if (value instanceof Integer) {
            return ((Integer) value).doubleValue();
        } else if (value instanceof Long) {
            return ((Long) value).doubleValue();
        }
        return null;
    }

    private static Float convertToFloat(Object value) {
        if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Double) {
            return ((Double) value).floatValue();
        } else if (value instanceof String) {
            try {
                return Float.parseFloat((String) value);
            } catch (NumberFormatException e) {
                System.out.println("Cannot convert string to float: " + value);
                return null;
            }
        } else if (value instanceof Integer) {
            return ((Integer) value).floatValue();
        } else if (value instanceof Long) {
            return ((Long) value).floatValue();
        }
        return null;
    }

    private static String convertToString(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        return value.toString();
    }

    private static Instant convertToInstant(Object value) {
        if (value == null) return null;

        if (value instanceof Instant) {
            return (Instant) value;
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
        } else if (value instanceof java.lang.Integer || value instanceof java.lang.Long) {
            long epochSeconds = value instanceof Integer
                    ? ((Integer) value).longValue()
                    : (Long) value;
            return new Instant(epochSeconds * 1000); // Convert seconds to milliseconds
        }

        return null;
    }

    private static Object convertToInstantIfApplicable(Object value) {
        if (value == null) return null;

        if (value instanceof Instant) {
            return value; // Return the Instant directly, not the milliseconds
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
        } else if (value instanceof java.lang.Integer || value instanceof java.lang.Long) {
            long epochSeconds = value instanceof Integer
                    ? ((Integer) value).longValue()
                    : (Long) value;
            return new Instant(epochSeconds * 1000); // Convert seconds to milliseconds
        }

        return value;
    }




}