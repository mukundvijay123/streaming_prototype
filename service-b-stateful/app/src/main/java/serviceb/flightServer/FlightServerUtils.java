package serviceb.flightServer;

import java.util.List;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class FlightServerUtils {


    public FlightServerUtils(){

    }
    
    public static void printArrowStream(VectorSchemaRoot table){
        Schema schema =table.getSchema();
        System.out.println("Schema: \n"+ schema);
        System.out.println("No of columns:" + schema.getFields().size());
        for (int i = 0; i < schema.getFields().size(); i++) {
            Field field = schema.getFields().get(i);
            System.out.println("Field " + i + ": " + field.getName() + " (" + field.getType() + ")");
        }

    }

    public static void printArrowTableConcise(VectorSchemaRoot root){
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            var vector = root.getFieldVectors().get(i);
            System.out.print("Column '" + vector.getField().getName() + "':");
            int rowsToPrint = Math.min(5, root.getRowCount());
            for (int row = 0; row < rowsToPrint; row++) {
                Object value = vector.getObject(row);
                System.out.print(value+",");
            }
            if (root.getRowCount() > 5) {
                System.out.println("  ... (" + (root.getRowCount() - 10) + " more rows)");
            }
            System.out.print("\n");
        }
    }
}
