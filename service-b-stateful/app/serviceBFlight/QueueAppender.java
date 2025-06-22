package org.example;

import net.openhft.chronicle.queue.ExcerptAppender;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class QueueAppender {

    public static void handleIncomingData(ExcerptAppender appender, FlightStream flightStream, FlightProducer.StreamListener<PutResult> ackStream) {
        System.out.println("=== HANDLING INCOMING DATA ===");
        System.out.println("Thread: " + Thread.currentThread().getName());

        try {
            VectorSchemaRoot root = flightStream.getRoot();
            Schema schema = root.getSchema();

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
                storeArrowData(appender, root, batchCount);
            }

            System.out.println("=== FINISHED PROCESSING " + batchCount + " BATCHES ===");
            ackStream.onNext(PutResult.empty());
            ackStream.onCompleted();

        } catch (Exception e) {
            System.err.println("Error handling incoming data: " + e.getMessage());
            e.printStackTrace();
            System.out.println(e.getMessage());
            ackStream.onError(e);
        }
    }
    static void storeArrowData(ExcerptAppender appender, VectorSchemaRoot root, int batchNumber) {

        try (ByteArrayOutputStream sink = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, sink)) {
            System.out.println("\n=== SERIALIZING AND STORING BATCH " + batchNumber + " IN CHRONICLE QUEUE ===");

            writer.start();
            writer.writeBatch();
            writer.end();

            byte[] serialized = sink.toByteArray();
            long messageSize = serialized.length;
            synchronized (appender) {
                appender.writeDocument(w -> {
                    w.write("arrowData").bytes(serialized);
                });
            }

            System.out.println("✓ Batch " + batchNumber + " stored in Chronicle Queue (" + messageSize + " bytes)");

        } catch (IOException e) {
            System.err.println("Error serializing/storing batch " + batchNumber + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}
