
package org.example;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChronicleQueueDequeueWorker implements Runnable {
    public static class QueueMessage {
        private final String topic;
        private final VectorSchemaRoot arrowData;
        // private final String timestamp;
        // private final int batchNumber;

        public QueueMessage(String topic, VectorSchemaRoot arrowData) {
            this.topic = topic;
            this.arrowData = arrowData;
            // this.timestamp = timestamp;
            // this.batchNumber = batchNumber;
        }

        public String getTopic() {
            return topic;
        }

        public VectorSchemaRoot getArrowData() {
            return arrowData;
        }

        @Override
        public String toString() {
            return "QueueMessage{" +
                    "topic='" + topic + '\'' +
                    ", data=" + (arrowData != null ? arrowData : 0) +
                    '}';
        }
    }

    private final ChronicleQueue chronicleQueue;
    private final AtomicBoolean isRunning;
    private final Map<String, List<BlockingQueue<QueueMessage>>> topicToQueues;
    private final BufferAllocator allocator;
    private static final int OFFER_TIMEOUT_SECONDS = 1;
    private static final int SLEEP_INTERVAL_MS = 10;

    public ChronicleQueueDequeueWorker(ChronicleQueue chronicleQueue,
                                       AtomicBoolean isRunning,
                                       Map<String, List<BlockingQueue<QueueMessage>>> topicToQueues) {
        this.chronicleQueue = chronicleQueue;
        this.isRunning = isRunning;
        this.topicToQueues = topicToQueues;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void run() {
        ExcerptTailer tailer = chronicleQueue.createTailer();
        System.out.println("Chronicle Queue dequeue thread started");

        while (isRunning.get()) {
            try {
                boolean hasMessage = tailer.readDocument(w -> {
                    try {
                        // Read only the arrowData from Chronicle Queue
                        byte[] arrowData = w.read("arrowData").bytes();
                        
                        if (arrowData == null || arrowData.length == 0) {
                            System.err.println("Received empty or null arrow data");
                            return;
                        }
                        
                        //reallocation?
                        ArrowStreamReader reader1 = new ArrowStreamReader(new ByteArrayInputStream(arrowData), allocator);
                        VectorSchemaRoot root1 = reader1.getVectorSchemaRoot();
                        
                        String topic = root1.getSchema().getCustomMetadata().get("topic");
                        // Create message object
                        QueueMessage message = new QueueMessage(topic, root1);
                        
                        System.out.println("Dequeued message for topic: " + topic + 
                                         ", data : \n" + arrowData);

                        // Distribute message to all queues registered for this topic
                        List<BlockingQueue<QueueMessage>> targetQueues = topicToQueues.get(topic);
                        if (targetQueues != null && !targetQueues.isEmpty()) {
                            int successfulOffers = 0;
                            for (BlockingQueue<QueueMessage> queue : targetQueues) {
                                try {
                                    boolean offered = queue.offer(message, OFFER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                                    if (offered) {
                                        successfulOffers++;
                                    } else {
                                        System.err.println("Failed to offer message to queue for topic: " + topic + 
                                                         " (queue may be full)");
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    System.err.println("Interrupted while offering message to queue for topic: " + topic);
                                    return; 
                                }
                            }
                            System.out.println("Distributed message for topic '" + topic + 
                                             "' to " + successfulOffers + "/" + targetQueues.size() + " queues");
                        } else {
                            System.err.println("No registered queues found for topic: " + topic);
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing message: " + e.getMessage());
                        e.printStackTrace();
                    }
                    
                });

                // If no message was available, sleep briefly to avoid busy waiting
                if (!hasMessage) {
                    Thread.sleep(SLEEP_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Dequeue thread interrupted, shutting down gracefully");
                break;
            } catch (Exception e) {
                System.err.println("Unexpected error in dequeue thread: " + e.getMessage());
                e.printStackTrace();
                // Continue processing other messages even if one fails
            }
        }
        
        // Clean up resources
        try {
            tailer.close();
        } catch (Exception e) {
            System.err.println("Error closing tailer: " + e.getMessage());
        }

        // Close the allocator
        try {
            allocator.close();
        } catch (Exception e) {
            System.err.println("Error closing allocator: " + e.getMessage());
        }

        System.out.println("Chronicle Queue dequeue thread stopped");
    }
}
