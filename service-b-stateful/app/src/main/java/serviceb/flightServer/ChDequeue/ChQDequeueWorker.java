package serviceb.flightServer.ChDequeue;

import java.io.ByteArrayInputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.VectorLoader;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import serviceb.Querying.QueryMetadata;
import serviceb.flightServer.FlightServerUtils;


//The old design was apparently leaking memory
public class ChQDequeueWorker implements Runnable {
    private final ChronicleQueue chronicleQueue;
    private final BufferAllocator allocator;
    private final AtomicBoolean isRunning;
    private final QueryMetadata metadataStore;
    private static final int SLEEP_INTERVAL_MS = 10;

    public ChQDequeueWorker(String chronicleQueuePath,
                            AtomicBoolean isRunning,
                            QueryMetadata metadataStore) {
        this.chronicleQueue = ChronicleQueue.singleBuilder(chronicleQueuePath).build();
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.isRunning = isRunning;
        this.metadataStore = metadataStore;
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
                                System.err.println("Received empty or null Arrow data");
                                return;
                            }

                            try (ArrowStreamReader reader = new ArrowStreamReader(
                                    new ByteArrayInputStream(arrowData), allocator)) {

                                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                                if (reader.loadNextBatch()) {
                                    String topic = root.getSchema()
                                            .getCustomMetadata()
                                            .getOrDefault("topic", "");
                                    //System.out.println(topic);
                                    // Clone the root
                                    VectorSchemaRoot clonedRoot = cloneRoot(root, allocator);
                                    //System.out.println(clonedRoot);
                                    //FlightServerUtils.printArrowTableConcise(clonedRoot);
                                    // Queue the clone
                                    metadataStore.supplyData(topic, clonedRoot);
                                } else {
                                    System.err.println("Arrow stream had no data batch");
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error processing message:");
                            e.printStackTrace();
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
                    System.err.println("Unexpected error in dequeue thread:");
                    e.printStackTrace();
                    break;
                }
            }
        } finally {
            try {
                tailer.close();
            } catch (Exception e) {
                System.err.println("Error closing tailer:");
                e.printStackTrace();
            }

            try {
                allocator.close();
            } catch (Exception e) {
                System.err.println("Error closing allocator:");
                e.printStackTrace();
            }

            System.out.println("Chronicle Queue dequeue thread stopped");
        }
    }

    private VectorSchemaRoot cloneRoot(VectorSchemaRoot original, BufferAllocator allocator) {
        VectorUnloader unloader = new VectorUnloader(original);
        ArrowRecordBatch recordBatch = unloader.getRecordBatch();

        VectorSchemaRoot newRoot = VectorSchemaRoot.create(original.getSchema(), allocator);
        VectorLoader loader = new VectorLoader(newRoot);
        loader.load(recordBatch);

        recordBatch.close(); // release resources
        return newRoot;
    }
}
