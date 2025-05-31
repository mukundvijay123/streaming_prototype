package org.example;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

public class mainClassForFlightServerB {
    public static void main(String[] args) {
        ServiceBFlightSubscriber serviceB = null;
        /*
        CLASS:
            ChronicleQueue
            Tailer
            HashMap ->
            {
            "<TOPIC>" : List <BlockingQueue>

            }
       a single thread looking at topic name in metadata and putting it in the respective queues
         */
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
}
