package serviceb.flightServer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

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
            //System.out.println(info);
            // Return the schema from the FlightInfo
            return info.getSchema();
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch schema for topic: " + topic, e);
        }
    }



    public static void subscribeToTopic(FlightClient flightClient, String consumerAddr, String topic, String token) {
        try {
            if (topic == null) {
                return;
            }

            // Construct JSON with nested auth.token
            String payload = String.format(
                "{ \"address\": \"%s\", \"topic\": \"%s\", \"auth\": { \"token\": \"%s\" } }",
                consumerAddr, topic, token
            );

            Action action = new Action("subscribe", payload.getBytes(StandardCharsets.UTF_8));
            Iterator<Result> results = flightClient.doAction(action);

            while (results.hasNext()) {
                Result result = results.next();
                String response = new String(result.getBody(), StandardCharsets.UTF_8);
                System.out.println("Subscription response for topic " + topic + ": " + response);
            }

        } catch (Exception e) {
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


    public static CompletableFuture<Boolean> checkAccessAsync(String baseUrl, String token, String topic, String action) {
        HttpClient client = HttpClient.newHttpClient();
        String url = String.format("%s/authorize?topic=%s&action=%s", baseUrl, topic, action);
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + token)
            .GET()
            .build();

        return client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
            .thenApply(response -> response.statusCode() == 200)
            .exceptionally(e -> {
                System.out.println("Error checking access: " + e.getMessage());
                return false;
            });
    }
}
