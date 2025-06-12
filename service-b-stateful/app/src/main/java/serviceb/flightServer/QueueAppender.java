package serviceb.flightServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import net.openhft.chronicle.queue.ExcerptAppender;

public class QueueAppender {
    public static void handleIncomingData(ExcerptAppender appender,
        FlightStream flightStream,
        FlightProducer.StreamListener<PutResult> ackStream){
        

        try{
            VectorSchemaRoot root =flightStream.getRoot();
           // System.out.println("===ARROW STREAM===");
            //FlightServerUtils.printArrowStream(root);
            Schema schema =root.getSchema();
            int batchCount=0;
            while(flightStream.next()){
                batchCount++;
                //System.out.println("\n--- Batch " + batchCount + " ---");
                //System.out.println("Row count: " + root.getRowCount());
                //FlightServerUtils.printArrowTableConcise(root);
                AppendToChq(appender, root, batchCount);
            }
            ackStream.onNext(PutResult.empty());
            ackStream.onCompleted();
            //System.out.println("===STREAM END===");
            
        }catch(Exception e){
            System.err.println("Error while processing stream"+ e.getMessage());
            e.printStackTrace();
            ackStream.onError(e);
        }
    }


    private static void AppendToChq(ExcerptAppender appender,VectorSchemaRoot root,int batchNumber){

        try(ByteArrayOutputStream sink =new ByteArrayOutputStream();
        ArrowStreamWriter writer =new ArrowStreamWriter(root, null, sink)){
            writer.start();
            writer.writeBatch();
            writer.end();

            byte[] serialized=sink.toByteArray();
            long messageSize =serialized.length;
            //Doing a thread safe write
            synchronized (appender) {
                appender.writeDocument(w -> {
                    w.write("arrowData").bytes(serialized);
                });
            }

        }catch(IOException e){
            System.err.println("Error serializing/storing batch " + batchNumber + ": " + e.getMessage());
            e.printStackTrace();            
        }
    }



}
