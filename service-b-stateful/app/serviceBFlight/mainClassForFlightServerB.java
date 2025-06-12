package org.example;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class mainClassForFlightServerB {
    private static final BlockingQueue<ChronicleQueueDequeueWorker.QueueMessage> queue1 = new LinkedBlockingQueue<>();
    private static final BlockingQueue<ChronicleQueueDequeueWorker.QueueMessage> queue2 = new LinkedBlockingQueue<>();
    private static final BlockingQueue<ChronicleQueueDequeueWorker.QueueMessage> queue3 = new LinkedBlockingQueue<>();
    private static final BlockingQueue<ChronicleQueueDequeueWorker.QueueMessage> queue4 = new LinkedBlockingQueue<>();
    public static void main(String[] args) {
        ServiceBFlightSubscriber serviceB = null;
        final Thread dequeueThread;
        final Thread printerThread;

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
            final ServiceBFlightSubscriber localServiceB = serviceB;

            List<String> topics = Arrays.asList("ABC", "XYZ");
            System.out.println("Starting to subscribe to topics: " + topics);
            serviceB.subscribeToTopics(topics);

            // Setup topic-to-queues map for dequeuing messages
            Map<String, List<BlockingQueue<ChronicleQueueDequeueWorker.QueueMessage>>> topicToQueues = new ConcurrentHashMap<>();
            
            List<BlockingQueue<ChronicleQueueDequeueWorker.QueueMessage>> allQueues = Arrays.asList(queue1, queue2, queue3, queue4);
            topicToQueues.put("ABC", Arrays.asList(queue1, queue2,queue3));
            topicToQueues.put("XYZ", Arrays.asList(queue4));
            System.out.println("Topics:" +topicToQueues.keySet());
            System.out.println("Values:"+topicToQueues.values());
            System.out.println(" Topic to queues mapping configured");

            ChronicleQueueDequeueWorker dequeueWorker = new ChronicleQueueDequeueWorker(
                    serviceB.chronicleQueue,
                    serviceB.isRunning,
                    topicToQueues
            );

            dequeueThread = new Thread(dequeueWorker, "Dequeue-Worker-Thread");
            dequeueThread.start();

            Runnable printerRunnable = () -> {
                while (localServiceB.isRunning()) {
                    for (String topic : topics) {
                        List<BlockingQueue<ChronicleQueueDequeueWorker.QueueMessage>> queues = topicToQueues.get(topic);
                        if (queues != null) {
                            for (BlockingQueue<ChronicleQueueDequeueWorker.QueueMessage> queue : queues) {
                                ChronicleQueueDequeueWorker.QueueMessage msg = queue.poll();
                                if (msg != null) {
                                    System.out.println("[PrinterThread] Received message:");
                                    System.out.println("  Topic: " + msg.getTopic());
                                    // System.out.println("  Timestamp: " + msg.getTimestamp());
                                    // System.out.println("  Batch Number: " + msg.getBatchNumber());
                                    System.out.println("  Arrow Data : " + msg.getArrowData());
                                    System.out.println("--------------------------------------");
                                }
                            }
                        }
                    }
                    try {
                        Thread.sleep(100);  // avoid busy waiting
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                System.out.println("Printer thread stopped.");
            };

            // Assign and start the printer thread
            printerThread = new Thread(printerRunnable, "Printer-Thread");
            printerThread.start();

            final ServiceBFlightSubscriber finalServiceB = serviceB;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("=== SHUTDOWN HOOK TRIGGERED ===");
                finalServiceB.shutdown();

                // Interrupt threads if running
                if (dequeueThread != null) {
                    dequeueThread.interrupt();
                }
                if (printerThread != null) {
                    printerThread.interrupt();
                }
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
