package org.example;
 
import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enhanced Service B with Chronicle Queue dequeuing to blocking queues
 */
public class ServiceBFlightSubscriber {
    private static final Logger logger = LoggerFactory.getLogger(ServiceBFlightSubscriber.class);
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // Chronicle Queue message structure
    public static class QueueMessage {
        public String timestamp;
        public int batchNumber;
        public byte[] arrowData;
        public String topic; // Added topic field
        
        public QueueMessage(String timestamp, int batchNumber, byte[] arrowData, String topic) {
            this.timestamp = timestamp;
            this.batchNumber = batchNumber;
            this.arrowData = arrowData;
            this.topic = topic;
        }
    }

    static {
        System.out.println("=== STATIC BLOCK: ServiceBFlightSubscriber class loading started ===");
        System.out.println("Current time: " + LocalDateTime.now());
        try {
            Logger staticLogger = LoggerFactory.getLogger(ServiceBFlightSubscriber.class);
            staticLogger.info("Static logger test successful");
            System.out.println("=== STATIC BLOCK: Logger initialized successfully ===");
        } catch (Exception e) {
            System.err.println("=== STATIC BLOCK ERROR: Logger initialization failed: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("=== STATIC BLOCK: ServiceBFlightSubscriber class loading completed ===");
    }

    public static void main(String[] args) {
        System.out.println("\n=== MAIN METHOD START ===");
        System.out.println("Current time: " + LocalDateTime.now());
        System.out.println("Java version: " + System.getProperty("java.version"));
        System.out.println("Available processors: " + Runtime.getRuntime().availableProcessors());
        System.out.println("Max memory: " + Runtime.getRuntime().maxMemory() / 1024 / 1024 + " MB");

        ServiceBFlightSubscriber serviceB = null;

        try {
            String serviceAAddress = System.getProperty("serviceA.address", "grpc://localhost:8815");
            String serviceBAddress = System.getProperty("serviceB.address", "grpc://localhost:8816");
            String chronicleQueuePath = System.getProperty("queue.path", "./service-b-queue");
            int serviceBPort = Integer.parseInt(System.getProperty("serviceB.port", "8816"));

            System.out.println("Service-A Address: " + serviceAAddress);
            System.out.println("Service-B Address: " + serviceBAddress);
            System.out.println("Service-B Port: " + serviceBPort);
            System.out.println("Chronicle Queue Path: " + chronicleQueuePath);

            serviceB = new ServiceBFlightSubscriber(
                    serviceAAddress, serviceBAddress, chronicleQueuePath, serviceBPort);

            List<String> topics = Arrays.asList("ABC", "XYZ");
            System.out.println("Starting to subscribe to topics: " + topics);
            serviceB.subscribeToTopics(topics);

            final ServiceBFlightSubscriber finalServiceB = serviceB;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("=== SHUTDOWN HOOK TRIGGERED ===");
                finalServiceB.shutdown();
            }));

            System.out.println("=== SERVICE RUNNING - Press Ctrl+C to stop ===");
            while (serviceB.isRunning()) {
                Thread.sleep(1000);
                System.out.println("Service is running... " + LocalDateTime.now());
            }

        } catch (Exception e) {
            System.err.println("=== MAIN METHOD ERROR ===");
            System.err.println("Error: " + e.getMessage());
            System.err.println("Error class: " + e.getClass().getName());
            e.printStackTrace();
            if (serviceB != null) {
                serviceB.shutdown();
            }
            System.exit(1);
        }
    }

    private final BufferAllocator allocator;
    private final FlightClient flightClient;
    private final ChronicleQueue chronicleQueue;
    private final String serviceAAddress;
    private final String serviceBAddress;
    private final ExecutorService executorService;
    private final Set<String> subscribedTopics;
    private final FlightServer flightServer;
    private final AtomicBoolean isRunning;
    private final ThreadLocal<ExcerptAppender> appenderThreadLocal;

    // Dequeuing components
    private final BlockingQueue<QueueMessage> queue1 = new LinkedBlockingQueue<>();
    private final BlockingQueue<QueueMessage> queue2 = new LinkedBlockingQueue<>();
    private final BlockingQueue<QueueMessage> queue3 = new LinkedBlockingQueue<>();
    private final BlockingQueue<QueueMessage> queue4 = new LinkedBlockingQueue<>();
    private final Map<String, List<BlockingQueue<QueueMessage>>> topicToQueues = new HashMap<>();
    private final ExecutorService dequeueExecutor;

    public ServiceBFlightSubscriber(String serviceAAddress, String serviceBAddress,
                                    String chronicleQueuePath, int serviceBPort) {
        System.out.println("\n=== CONSTRUCTOR START ===");
        System.out.println("Current time: " + LocalDateTime.now());

        try {
            this.allocator = new RootAllocator(Long.MAX_VALUE);
            this.serviceAAddress = serviceAAddress;
            this.serviceBAddress = serviceBAddress;
            this.executorService = Executors.newFixedThreadPool(1);
            this.subscribedTopics = ConcurrentHashMap.newKeySet();
            this.isRunning = new AtomicBoolean(true);
            this.dequeueExecutor = Executors.newSingleThreadExecutor();

            this.chronicleQueue = SingleChronicleQueueBuilder.binary(chronicleQueuePath).build();
            this.appenderThreadLocal = ThreadLocal.withInitial(() -> chronicleQueue.createAppender());

            // Setup topic to queues mapping
            setupTopicToQueuesMapping();

            URI serviceAUri = URI.create(serviceAAddress);
            Location serviceALocation = Location.forGrpcInsecure(serviceAUri.getHost(), serviceAUri.getPort());
            this.flightClient = FlightClient.builder(allocator, serviceALocation).build();

            this.flightServer = startFlightServer(serviceBPort);

            // Start dequeue thread
            startDequeueThread();

            logger.info("[{}] ServiceBFlightSubscriber initialized successfully", getCurrentTimestamp());
            System.out.println("=== CONSTRUCTOR COMPLETED SUCCESSFULLY ===");

        } catch (Exception e) {
            System.err.println("=== CONSTRUCTOR ERROR ===");
            System.err.println("Error in constructor: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Constructor failed", e);
        }
    }

    private void setupTopicToQueuesMapping() {
        // Hard-coded mapping: each topic goes to all 4 queues
        List<BlockingQueue<QueueMessage>> allQueues = Arrays.asList(queue1, queue2, queue3, queue4);
        topicToQueues.put("ABC", Arrays.asList(queue1, queue2,queue3));
        topicToQueues.put("XYZ", Arrays.asList(queue4));
        System.out.println(" Topic to queues mapping configured");
    }

    private void startDequeueThread() {
        dequeueExecutor.submit(() -> {
            ExcerptTailer tailer = chronicleQueue.createTailer();
            System.out.println(" Chronicle Queue dequeue thread started");

            while (isRunning.get()) {
                try {
                    boolean hasMessage = tailer.readDocument(w -> {
                        String timestamp = w.read("timestamp").text();
                        int batchNumber = w.read("batchNumber").int32();
                        byte[] arrowData = w.read("arrowData").bytes();
                        String topic = w.read("topic").text(); // Read topic from queue

                        QueueMessage message = new QueueMessage(timestamp, batchNumber, arrowData, topic);
                        
                        List<BlockingQueue<QueueMessage>> targetQueues = topicToQueues.get(topic);
                        if (targetQueues != null) {
                            for (BlockingQueue<QueueMessage> queue : targetQueues) {
                                try {
                                    queue.offer(message, 1, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                            System.out.println(" Dequeued and distributed message for topic: " + topic + 
                                             ", batch: " + batchNumber + " to " + targetQueues.size() + " queues");
                        }
                    });

                    if (!hasMessage) {
                        Thread.sleep(10); // Brief pause if no message
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("Error in dequeue thread: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            System.out.println(" Chronicle Queue dequeue thread stopped");
        });
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
                return () -> handleIncomingData(flightStream, ackStream);
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

    private void handleIncomingData(FlightStream flightStream, StreamListener<PutResult> ackStream) {
        System.out.println("=== HANDLING INCOMING DATA ===");
        System.out.println("Time: " + getCurrentTimestamp());
        System.out.println("Thread: " + Thread.currentThread().getName());

        try {
            VectorSchemaRoot root = flightStream.getRoot();
            
            Schema schema = root.getSchema();
            System.out.println("Metadata: "+ schema.getCustomMetadata());

            System.out.println("\n=== ARROW TABLE RECEIVED ===");
            System.out.println("Schema: " + schema);
            System.out.println("Number of fields: " + schema.getFields().size());

            for (int i = 0; i < schema.getFields().size(); i++) {
                Field field = schema.getFields().get(i);
                System.out.println("Field " + i + ": " + field.getName() + " (" + field.getType() + ")");
            }

            int batchCount = 0;
            while (flightStream.next()) {
                batchCount++;
                System.out.println("\n--- Batch " + batchCount + " ---");
                System.out.println("Row count: " + root.getRowCount());

                for (int i = 0; i < root.getFieldVectors().size(); i++) {
                    var vector = root.getFieldVectors().get(i);
                    System.out.println("Column '" + vector.getField().getName() + "':");
                    int rowsToPrint = Math.min(10, root.getRowCount());
                    for (int row = 0; row < rowsToPrint; row++) {
                        Object value = vector.getObject(row);
                        System.out.println("  Row " + row + ": " + value);
                    }
                    if (root.getRowCount() > 10) {
                        System.out.println("  ... (" + (root.getRowCount() - 10) + " more rows)");
                    }
                }

                // Extract topic from data or use default
                String topic = extractTopicFromData(root);
                storeArrowData(root, batchCount, topic);
            }

            System.out.println("=== FINISHED PROCESSING " + batchCount + " BATCHES ===");
            ackStream.onNext(PutResult.empty());
            ackStream.onCompleted();

        } catch (Exception e) {
            System.err.println("Error handling incoming data: " + e.getMessage());
            e.printStackTrace();
            ackStream.onError(e);
        }
    }

    private String extractTopicFromData(VectorSchemaRoot root) {
    // Try to extract topic from custom metadata, fallback to default
    Schema schema = root.getSchema();
    var metadata = schema.getCustomMetadata();

    if (metadata != null && metadata.containsKey("topic")) {
        String topic = metadata.get("topic");
        if (topic != null && !topic.isEmpty()) {
            System.out.println("Extracted topic from metadata: " + topic);
            return topic;
        }
    }

    return "No Topic"; // Default topic
}


    private void storeArrowData(VectorSchemaRoot root, int batchNumber, String topic) {
        ExcerptAppender appender = appenderThreadLocal.get();
        String timestamp = getCurrentTimestamp();

        try (ByteArrayOutputStream sink = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, sink)) {
            System.out.println("\n=== SERIALIZING AND STORING BATCH " + batchNumber + " IN CHRONICLE QUEUE ===");

            writer.start();
            writer.writeBatch();
            writer.end();

            byte[] serialized = sink.toByteArray();
            long messageSize = serialized.length;

            appender.writeDocument(w -> {
                w.write("timestamp").text(timestamp);
                w.write("batchNumber").int32(batchNumber);
                w.write("arrowData").bytes(serialized);
                w.write("topic").text(topic); // Store topic in queue
            });

            System.out.println(" Batch " + batchNumber + " for topic " + topic + " stored in Chronicle Queue (" + messageSize + " bytes)");

        } catch (IOException e) {
            System.err.println("Error serializing/storing batch " + batchNumber + ": " + e.getMessage());
            e.printStackTrace();
        }
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
                String payload = String.format("{\"address\": \"%s\", \"topic\": \"%s\"}", serviceBAddress, topic);
                Action action = new Action("subscribe", payload.getBytes(StandardCharsets.UTF_8));
                Iterator<Result> results = flightClient.doAction(action);

                while (results.hasNext()) {
                    Result result = results.next();
                    String response = new String(result.getBody(), StandardCharsets.UTF_8);
                    System.out.println("Subscription response for topic " + topic + ": " + response);
                }
                subscribedTopics.add(topic);
                System.out.println(" Subscribed to topic: " + topic);
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

    // Getters for accessing blocking queues (for testing/monitoring)
    public BlockingQueue<QueueMessage> getQueue1() { return queue1; }
    public BlockingQueue<QueueMessage> getQueue2() { return queue2; }
    public BlockingQueue<QueueMessage> getQueue3() { return queue3; }
    public BlockingQueue<QueueMessage> getQueue4() { return queue4; }

    public void shutdown() {
        System.out.println("\n=== SHUTDOWN START ===");
        isRunning.set(false);

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
                    System.out.println(" Unsubscribed from topic: " + topic);
                } catch (Exception e) {
                    System.err.println("Error unsubscribing from topic " + topic + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }
            subscribedTopics.clear();

            if (dequeueExecutor != null) {
                dequeueExecutor.shutdown();
                if (!dequeueExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    dequeueExecutor.shutdownNow();
                }
                System.out.println(" Dequeue executor shutdown");
            }

            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
                System.out.println(" Executor service shutdown");
            }

            if (flightServer != null) {
                flightServer.close();
                System.out.println(" Flight server closed");
            }

            if (flightClient != null) {
                flightClient.close();
                System.out.println(" Flight client closed");
            }

            if (chronicleQueue != null) {
                chronicleQueue.close();
                System.out.println(" Chronicle Queue closed");
            }

            if (allocator != null) {
                allocator.close();
                System.out.println(" Buffer allocator closed");
            }

            appenderThreadLocal.remove();
            System.out.println(" ThreadLocal appender cleaned up");

            System.out.println("=== SHUTDOWN COMPLETED ===");
        } catch (Exception e) {
            System.err.println("Error during shutdown: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String getCurrentTimestamp() {
        return LocalDateTime.now().format(TIMESTAMP_FORMAT);
    }
}
