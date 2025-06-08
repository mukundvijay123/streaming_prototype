package serviceb.flightServer;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Result;

public class StreamSubscribeUtils {
    
    public StreamSubscribeUtils(){

    }

    public static void subscribeToTopic(FlightClient flightClient,String BrokerAddr,String topic){
        try{
            if(topic==null){
                return;
            }
            String payload =String.format("{\"address\": \"%s\", \"topic\": \"%s\"}",BrokerAddr,topic);
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

    public void unsubscribeToTopic(FlightClient flightClient,String MyAddr,String topic){
    try{
        if(topic==null){
            return;
        }
        String payload =String.format("{\"address\": \"%s\", \"topic\": \"%s\"}",MyAddr,topic);
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
