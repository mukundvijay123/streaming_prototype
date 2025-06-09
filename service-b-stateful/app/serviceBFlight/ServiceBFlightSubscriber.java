package org.example;


import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Debug Version - Enhanced Service B that subscribes to Python Service-A via Arrow Flight
 * and stores received Arrow data in Chronicle Queue (Thread-Safe Version)
 */
public class ServiceBFlightSubscriber {
   // private final ExecutorService executorService;
    public final Set<String> subscribedTopics;
    public final FlightServer flightServer;
    public final AtomicBoolean isRunning;
    public final ThreadLocal<ExcerptAppender> appenderThreadLocal;

    public ServiceBFlightSubscriber(String serviceAAddress, String serviceBAddress,
                                    String chronicleQueuePath, int serviceBPort) {
        System.out.println("\n=== CONSTRUCTOR START ===");
        System.out.println("Current time: " + LocalDateTime.now());

        try {
            this.allocator = new RootAllocator(Long.MAX_VALUE);
            this.serviceAAddress = serviceAAddress;
            this.serviceBAddress = serviceBAddress;
         //   this.executorService = Executors.newFixedThreadPool(1);
            this.subscribedTopics = ConcurrentHashMap.newKeySet();
            this.isRunning = new AtomicBoolean(true);

            this.chronicleQueue = SingleChronicleQueueBuilder.binary(chronicleQueuePath).build();
            this.appenderThreadLocal = ThreadLocal.withInitial(() -> chronicleQueue.createAppender());

            URI serviceAUri = URI.create(serviceAAddress);
            Location serviceALocation = Location.forGrpcInsecure(serviceAUri.getHost(), serviceAUri.getPort());
            this.flightClient = FlightClient.builder(allocator, serviceALocation).build();

            this.flightServer = startFlightServer(serviceBPort);

            System.out.println("=== CONSTRUCTOR COMPLETED SUCCESSFULLY ===");

        } catch (Exception e) {
            System.err.println("=== CONSTRUCTOR ERROR ===");
            System.err.println("Error in constructor: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Constructor failed", e);
        }
    }

    private FlightServer startFlightServer(int serviceBPort) throws Exception {
        System.out.println("    Creating Flight Server location...");
        Location serverLocation = Location.forGrpcInsecure("127.0.0.1", serviceBPort);
        System.out.println("    Creating Flight Producer...");

        FlightProducer producer = new FlightProducer() {
            @Override
            public Runnable acceptPut(CallContext context, FlightStream flightStream,
                                      StreamListener<PutResult> ackStream) {
                System.out.println("acceptPut called for context: " + context);
                return () -> QueueAppender.handleIncomingData(appenderThreadLocal.get(), flightStream, ackStream);
            }

            @Override
            public void getStream(CallContext context, Ticket ticket,
                                  ServerStreamListener listener) {
                System.out.println("    Flight Producer: getStream called for ticket: " +
                        new String(ticket.getBytes(), StandardCharsets.UTF_8));
                listener.completed();
            }

            @Override
            public void listFlights(CallContext context, Criteria criteria,
                                    StreamListener<FlightInfo> listener) {
                System.out.println("    Flight Producer: listFlights called");
                listener.onCompleted();
            }

            @Override
            public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
                System.out.println("    Flight Producer: getFlightInfo called");
                return null;
            }

            @Override
            public void doAction(CallContext context, Action action,
                                 StreamListener<Result> listener) {
                System.out.println("    Flight Producer: doAction called - " + action.getType());
                listener.onCompleted();
            }

            @Override
            public void listActions(CallContext context, StreamListener<ActionType> listener) {
                System.out.println("    Flight Producer: listActions called");
                listener.onCompleted();
            }
        };

        System.out.println("    Building Flight Server...");
        FlightServer server = FlightServer.builder(allocator, serverLocation, producer).build();
        System.out.println("    Starting Flight Server...");
        server.start();
        System.out.println("    Flight Server started successfully on: " + serverLocation);
        return server;
    }

    public void subscribeToTopics(List<String> topics) {
        System.out.println("\n=== SUBSCRIBE TO TOPICS START ===");
        System.out.println("Topics to subscribe to: " + topics);

        if (topics == null || topics.isEmpty()) {
            System.out.println("No topics provided, returning");
            return;
        }

        for (String topic : topics) {
            System.out.println("Subscribing to topic: " + topic);
            try {
                // Create subscription action payload
                String payload = String.format("{\"address\": \"%s\", \"topic\": \"%s\"}", serviceBAddress, topic);
                Action action = new Action("subscribe", payload.getBytes(StandardCharsets.UTF_8));
                Iterator<Result> results = flightClient.doAction(action);

                while (results.hasNext()) {
                    Result result = results.next();
                    String response = new String(result.getBody(), StandardCharsets.UTF_8);
                    System.out.println("Subscription response for topic " + topic + ": " + response);
                }
                subscribedTopics.add(topic);
                System.out.println("✓ Subscribed to topic: " + topic);
            } catch (Exception e) {
                System.err.println("Error subscribing to topic " + topic + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        System.out.println("=== SUBSCRIBE TO TOPICS COMPLETED ===");
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    public void shutdown() {
        System.out.println("\n=== SHUTDOWN START ===");
        //isRunning.set(false);

        try {
            // Unsubscribe from all topics
            for (String topic : subscribedTopics) {
                try {
                    String payload = String.format("{\"address\": \"%s\", \"topic\": \"%s\"}", serviceBAddress, topic);
                    Action action = new Action("unsubscribe", payload.getBytes(StandardCharsets.UTF_8));
                    Iterator<Result> results = flightClient.doAction(action);
                    while (results.hasNext()) {
                        Result result = results.next();
                        String response = new String(result.getBody(), StandardCharsets.UTF_8);
                        System.out.println("Unsubscription response for topic " + topic + ": " + response);
                    }
                    System.out.println("✓ Unsubscribed from topic: " + topic);
                } catch (Exception e) {
                    System.err.println("Error unsubscribing from topic " + topic + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }
            subscribedTopics.clear();
            if (flightServer != null) {
                flightServer.close();
            }
            if (flightClient != null) {
                flightClient.close();
            }
            if (chronicleQueue != null) {
                chronicleQueue.close();
            }
            if (allocator != null) {
                allocator.close();
            }
            appenderThreadLocal.remove();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
