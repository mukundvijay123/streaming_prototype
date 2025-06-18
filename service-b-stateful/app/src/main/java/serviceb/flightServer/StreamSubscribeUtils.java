package serviceb.flightServer;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Result;
import org.apache.arrow.vector.types.pojo.Schema;

public class StreamSubscribeUtils {
    
    public StreamSubscribeUtils(){

    }
    


    public static Schema fetchSchema(String topic, FlightClient client) {
        // Create a descriptor using the topic as a path
        FlightDescriptor descriptor = FlightDescriptor.path(Collections.singletonList(topic));

        try {
            // Get FlightInfo from the server using the descriptor
            FlightInfo info = client.getInfo(descriptor);
            // Return the schema from the FlightInfo
            return info.getSchema();
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch schema for topic: " + topic, e);
        }
    }



    public static void subscribeToTopic(FlightClient flightClient,String ConsumerAddr,String topic){
        try{
            if(topic==null){
                return;
            }
            String payload =String.format("{\"address\": \"%s\", \"topic\": \"%s\"}",ConsumerAddr,topic);
            Action action=new Action("subscribe",payload.getBytes(StandardCharsets.UTF_8));
            Iterator<Result> results = flightClient.doAction(action);
            while(results.hasNext()){
                Result result = results.next();
                String response = new String(result.getBody(), StandardCharsets.UTF_8);
                System.out.println("Subscription response for topic " + topic + ": " + response);
            }
        }catch(Exception e){
            System.err.println("Error subscribing to topic " + topic + ": " + e.getMessage());
            e.printStackTrace();
        }

    }

    public static void unsubscribeToTopic(FlightClient flightClient,String ConsumerAddr,String topic){
    try{
        if(topic==null){
            return;
        }
        String payload =String.format("{\"address\": \"%s\", \"topic\": \"%s\"}",ConsumerAddr,topic);
        Action action=new Action("unsubscribe",payload.getBytes(StandardCharsets.UTF_8));
        Iterator<Result> results = flightClient.doAction(action);
        while(results.hasNext()){
            Result result = results.next();
            String response = new String(result.getBody(), StandardCharsets.UTF_8);
            System.out.println("Unsubscription response for topic " + topic + ": " + response);
        }
    }catch(Exception e){
        System.err.println("Error subscribing to topic " + topic + ": " + e.getMessage());
        e.printStackTrace();
    }

    }
}
