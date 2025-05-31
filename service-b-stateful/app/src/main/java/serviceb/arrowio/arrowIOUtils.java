/*
 * THIS PACKAGE IS NOT THREAD SAFE
*/
package serviceb.arrowio;

import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.schemas.Schema;



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
    // public Row FixTimeFields(Row row){

    // }

}
