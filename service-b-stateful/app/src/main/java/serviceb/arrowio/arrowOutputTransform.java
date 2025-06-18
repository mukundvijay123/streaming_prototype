package serviceb.arrowio;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

/**
 * A DoFn that enqueues each Row as a String into arrowOutputTransform.outputQueue,
 * then passes the Row through unchanged.
 */
public class arrowOutputTransform extends DoFn<Row, Row> {
    public static final BlockingQueue<outputMessage> outputQueue =new LinkedBlockingQueue<>();
    private final String querySession;
    private final long offerTimeoutMs;

    public arrowOutputTransform(String querySession, long offerTimeoutMs) {
        this.querySession   = querySession;
        this.offerTimeoutMs = offerTimeoutMs;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        Row row = ctx.element();

        // convert Row to String (you can swap toJsonString or toCsvString if you like)
        String rowText = row.toString();

        System.out.println("this is good 1");
        // wrap in your outputMessage
        outputMessage msg = new outputMessage(querySession, rowText);

        try {
            boolean enqueued = arrowOutputTransform.outputQueue
                .offer(msg, offerTimeoutMs, TimeUnit.MILLISECONDS);
            if (!enqueued) {
                // queue was full / timeout expired
                System.err.println("Dropped message for session " + querySession);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while enqueueing", e);
        }

        // emit the original Row so downstream work continues
        ctx.output(row);
    }
}
