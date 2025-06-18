package serviceb;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import serviceb.Querying.QueryMetadata;
import serviceb.Querying.QueryResultBroadcasterWorker;
import serviceb.flightServer.ServiceBFlightConsumer;
import serviceb.flightServer.ChDequeue.ChQDequeueWorker;
import serviceb.wsServer.WebsocketServer;

public class App {

    public static void main(String[] args) {
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        try {
            String brokerAddress = "grpc://127.0.0.1:8815";
            String myAddress = "grpc://127.0.0.1:8818";
            String chronicleQueuePath = "service-b-queue";

            BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            AtomicBoolean running = new AtomicBoolean(true);

            QueryMetadata metadata = new QueryMetadata(allocator, myAddress, brokerAddress);

            ServiceBFlightConsumer flightServer = new ServiceBFlightConsumer(running, chronicleQueuePath, 8818);
            WebsocketServer websocketServer = new WebsocketServer("localhost",8767,metadata);
            ChQDequeueWorker dequeueWorker = new ChQDequeueWorker(chronicleQueuePath, running, metadata);
            QueryResultBroadcasterWorker resultBroadcasterWorker = new QueryResultBroadcasterWorker(metadata);

            flightServer.start();
            websocketServer.start();

            new Thread(dequeueWorker, "DequeueWorker").start();
            new Thread(resultBroadcasterWorker, "ResultBroadcasterWorker").start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutdown signal received. Cleaning up...");
                running.set(false); // Signal workers to stop
                try {
                    metadata.deleteQueryMetadata(); // cleanup metadata and allocator
                    flightServer.shutdown();           // clean gRPC server
                    websocketServer.stop();  
                    running.set(false);       // if your websocket server has a stop method
                } catch (Exception e) {
                    e.printStackTrace();
                }
                shutdownLatch.countDown();
            }));

            // Block the main thread until shutdown
            shutdownLatch.await();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Fatal error: " + e.getMessage());
        }
    }
}
