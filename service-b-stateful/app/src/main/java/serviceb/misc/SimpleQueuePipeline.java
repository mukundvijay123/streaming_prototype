package serviceb.misc;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;




import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;




public class SimpleQueuePipeline {



    private static final Schema ROW_SCHEMA = Schema.builder()
            .addInt32Field("k")
            .addInt32Field("n")
            .addStringField("value")
            .addDateTimeField("event_time")
            .build();




    /**
     * Simple checkpoint mark implementation
     */
    public static class SimpleCheckpoint implements UnboundedSource.CheckpointMark, Serializable {
        public SimpleCheckpoint() {
            //no checkpointing so far
        }

        @Override
        public void finalizeCheckpoint() throws IOException {
        }
    }




    /**
     * Unbounded source implementation that produces Row objects directly
     */
    public static class RowSource extends UnboundedSource<Row, SimpleCheckpoint> {
        // Static queue that can be accessed from outside the pipeline
        private static final BlockingQueue<Row> QUEUE = new LinkedBlockingQueue<>();

        // Method to add data to the queue
        public static void addToQueue(int k, int n, String value) {
            Row row = Row.withSchema(ROW_SCHEMA)
                    .withFieldValue("k", k)
                    .withFieldValue("n", n)
                    .withFieldValue("value", value)
                    .withFieldValue("event_time", Instant.now())
                    .build();
            //WE GET THIS DATA FROM ARROW FLIGHT
            QUEUE.add(row);
        }





        @Override
        public List<? extends UnboundedSource<Row, SimpleCheckpoint>> split(
                int desiredNumSplits, PipelineOptions options) {
            // for simplicity, don't split -> no splittung
            return List.of(this);
        }

        @Override 
        public UnboundedReader<Row> createReader(
                PipelineOptions options, SimpleCheckpoint checkpointMark) {
            return new RowReader(this);
        }

        @Override
        public Coder<SimpleCheckpoint> getCheckpointMarkCoder() {
            return SerializableCoder.of(SimpleCheckpoint.class);
        }

        @Override
        public Coder<Row> getOutputCoder() {
            return org.apache.beam.sdk.coders.RowCoder.of(ROW_SCHEMA);
        }

        /**
         * Reader implementation for the row source
         */
        private static class RowReader extends UnboundedReader<Row> {
            private final RowSource source;
            private Row current;
            private Instant watermark;

            public RowReader(RowSource source) {
                this.source = source;
                this.watermark = Instant.now();
            }






            @Override
            public boolean start() throws IOException {
                return advance();
            }

            @Override
            public boolean advance() throws IOException {
                try {
                    // Block until data is available (with 100ms timeout)
                    current = QUEUE.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (current != null) {
                        watermark = current.getDateTime("event_time").toInstant();
                        return true;
                    }
                    return false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while reading from queue", e);
                }
            }

            @Override
            public Row getCurrent() throws NoSuchElementException {
                if (current == null) {
                    throw new NoSuchElementException("No current element");
                }
                return current;
            }

            @Override
            public Instant getCurrentTimestamp() throws NoSuchElementException {
                if (current == null) {
                    throw new NoSuchElementException("No current element");
                }
                return current.getDateTime("event_time").toInstant();
            }

            @Override
            public void close() {
                // Nothing to close
            }

            @Override
            public Instant getWatermark() {
                return watermark;
            }

            @Override
            public SimpleCheckpoint getCheckpointMark() {
                return new SimpleCheckpoint();
            }

            @Override
            public UnboundedSource<Row, ?> getCurrentSource() {
                return source;
            }
        }
    }







    /**
     * DoFn to display SQL query results dynamically, without hardcoding field names
     */
    public static class DisplaySqlResultsDoFn extends DoFn<Row, Void> {
        @ProcessElement
        public void processElement(@Element Row row, OutputReceiver<Void> out) {
            StringBuilder sb = new StringBuilder();
            sb.append("\n======================================\n");
            sb.append("SQL WINDOW RESULT:\n");

            Schema schema = row.getSchema();
            for (Schema.Field field : schema.getFields()) {
                String fieldName = field.getName();
                Object fieldValue = row.getValue(fieldName);
                sb.append(fieldName).append(": ").append(fieldValue).append("\n");
            }

            sb.append("======================================\n");
            System.out.println(sb.toString());
        }
    }

    public static void main(String[] args) {
        Thread producerThread = new Thread(() -> {
            int counter = 0;
            Random random = new Random();
            while (true) {
                int k = random.nextInt(3) + 1;
                int n = random.nextInt(100);
                RowSource.addToQueue(k, n, "Data item " + counter++);
                try {
                    int t = random.nextInt(100);//in microseconds -> random events
                    Thread.sleep(t);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        producerThread.setDaemon(true);
        producerThread.start();

        PipelineOptions options = PipelineOptionsFactory.create();
        DirectOptions directOptions = options.as(DirectOptions.class);
        directOptions.setTargetParallelism(1); // Set desired number of threads
        Pipeline pipeline = Pipeline.create(directOptions);



        PCollection<Row> input = pipeline.apply("ReadFromQueue", Read.from(new RowSource()))
                .setRowSchema(ROW_SCHEMA);

//        PCollection<Row> windowed = input.apply(
//                "Window", Window.into(FixedWindows.of(Duration.standardSeconds(10))));

        PCollection<Row> sqlResult = input.apply(
                "SQL Window Aggregation",
                SqlTransform.query(
                        "SELECT " +
                                "  k, " +
                                "  COUNT(*) AS cnt, " +
                                "  MAX(n) AS max_n, AVG(n) as AVERAGE_OF_N, " +
                                "  TUMBLE_START(event_time, INTERVAL '10' SECOND) AS window_start, " +
                                "  TUMBLE_END(event_time, INTERVAL '10' SECOND) AS window_end " +
                                "FROM PCOLLECTION " +
                                "GROUP BY " +
                                "k," +
                                "  TUMBLE(event_time, INTERVAL '10' SECOND)")
        );

        sqlResult.apply("DisplayResults", ParDo.of(new DisplaySqlResultsDoFn()));
        pipeline.run();
    }
}