package serviceb.flightServer.ChDequeue;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;

public class ChQDequeueWorker implements Runnable{
    private final ChronicleQueue chronicleQueue;
    private final BufferAllocator allocator;
    private final AtomicBoolean isRunning;
    private final Map<String, List<BlockingQueue<QueueMessage>>> MetadataStore;//Temporary Queue Store Will be replaced later
    private static final int OFFER_TIMEOUT_SECONDS = 1;
    private static final int SLEEP_INTERVAL_MS = 10;

    public ChQDequeueWorker(ChronicleQueue chronicleQueue,
                                       AtomicBoolean isRunning,
                                       Map<String, List<BlockingQueue<QueueMessage>>> topicToQueues) {
        this.chronicleQueue = chronicleQueue;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.MetadataStore=topicToQueues;
        this.isRunning=isRunning;

    }

    @Override
    public void run() {
        ExcerptTailer tailer = chronicleQueue.createTailer();
        System.out.println("Chronicle Queue dequeue thread started");
        try {
            while (isRunning.get()) {
                try {
                    boolean hasMessage = tailer.readDocument(w -> {
                        try {
                            byte[] arrowData = w.read("arrowData").bytes();
                            if (arrowData == null || arrowData.length == 0) {
                                System.err.println("Received empty or null arrow data");
                                return;
                            }

                            try (ArrowStreamReader reader1 = new ArrowStreamReader(
                                    new ByteArrayInputStream(arrowData), allocator)) {
                                VectorSchemaRoot root1 = reader1.getVectorSchemaRoot();
                                reader1.loadNextBatch(); // Required to populate root1
                                String topic = root1.getSchema().getCustomMetadata().get("topic");

                                QueueMessage message = new QueueMessage(topic, root1);
                                List<BlockingQueue<QueueMessage>> targetQueues = MetadataStore.get(topic);
                                if (targetQueues != null && !targetQueues.isEmpty()) {
                                    int successfulOffers = 0;
                                    for (BlockingQueue<QueueMessage> queue : targetQueues) {
                                        try {
                                            boolean offered = queue.offer(message, OFFER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                                            if (offered) {
                                                successfulOffers++;
                                            } else {
                                                System.err.println("Queue full for topic: " + topic);
                                            }
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            System.err.println("Interrupted while offering message to queue: " + topic);
                                            return;
                                        }
                                    }
                                    System.out.println("Distributed message for topic '" + topic +
                                            "' to " + successfulOffers + "/" + targetQueues.size() + " queues");
                                } else {
                                    System.err.println("No queues found for topic: " + topic);
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error processing message: " + e.getMessage());
                        }
                    });

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
                    break; // Optional: Break on fatal errors
                }
            }
        } finally {
            try {
                tailer.close();
            } catch (Exception e) {
                System.err.println("Error closing tailer: " + e.getMessage());
            }

            try {
                allocator.close();
            } catch (Exception e) {
                System.err.println("Error closing allocator: " + e.getMessage());
            }

            System.out.println("Chronicle Queue dequeue thread stopped");
        }
    }  


}
