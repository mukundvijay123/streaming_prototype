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

import serviceb.utils.context;

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



    public static void subscribeToTopic(FlightClient flightClient, String consumerAddr, String topic, context ctx) throws Exception {
        if (topic == null) {
            return;
        }

        String payload = String.format(
            "{ \"address\": \"%s\", \"topic\": \"%s\", \"auth\": { \"token\": \"%s\", \"action\":\"%s\" } }",
            consumerAddr, topic, ctx.JWTToken,ctx.action
        );

        Action action = new Action("subscribe", payload.getBytes(StandardCharsets.UTF_8));
        Iterator<Result> results = flightClient.doAction(action);

        boolean gotResponse = false;

        while (results.hasNext()) {
            gotResponse = true;
            Result result = results.next();
            String response = new String(result.getBody(), StandardCharsets.UTF_8);
            System.out.println("Subscription response for topic " + topic + ": " + response);

            // Optional: check if the response indicates a failure
            if (response.toLowerCase().contains("error") || response.toLowerCase().contains("fail")) {
                throw new Exception("Subscription failed for topic " + topic + ": " + response);
            }
        }

        // If no results were returned at all
        if (!gotResponse) {
            throw new Exception("No response received while subscribing to topic " + topic);
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
    System.out.println("this is good");
    HttpClient client = HttpClient.newHttpClient();
    System.out.println("action:"+action);
    String url = String.format("%s/authorize?topic=%s&action=%s", baseUrl, topic, action);
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Authorization", "Bearer " + token)
        .GET()
        .build();

    return client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
        .thenApply(response -> {
            int status = response.statusCode();
            return status >= 200 && status < 300;
        })
        .exceptionally(e -> {
            System.out.println("Error checking access for topic '" + topic + "': " + e.getMessage());
            return false;
        });
    }
}