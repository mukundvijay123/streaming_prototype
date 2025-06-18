package serviceb.flightServer;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;


import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class ServiceBFlightConsumer{
    public final FlightServer flightServer;
    private final BufferAllocator allocator;
    public  final AtomicBoolean isRunning;
    public final ChronicleQueue chronicleQueue;
    private final ThreadLocal<ExcerptAppender> appenderThreadLocal;
;


    //Constructor
    public ServiceBFlightConsumer(AtomicBoolean Run,String chronicleQueuePath, int serviceBPort){
        this.allocator=new RootAllocator(Long.MAX_VALUE);
        this.chronicleQueue=SingleChronicleQueueBuilder.single(chronicleQueuePath).build();
        this.appenderThreadLocal =ThreadLocal.withInitial(() -> chronicleQueue.createAppender());
        this.isRunning=Run;


        

        try{
            this.flightServer=createFlightServer(serviceBPort);
        }catch(Exception e){
            System.err.println("=== CONSTRUCTOR ERROR ===");
            System.err.println("Error in constructor: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Constructor failed", e);
        }
    }

    public final boolean isRunning(){
        return this.isRunning.get();
    }


    //Creating a Flight Server 
    private FlightServer createFlightServer(int serviceBPort) throws Exception{
        System.out.println("    Creating Flight Server location...");
        Location serverLocation = Location.forGrpcInsecure("127.0.0.1", serviceBPort);
        System.out.println("    Creating Flight Server...");

        FlightProducer producer =new FlightProducer() {
            @Override
            public Runnable acceptPut(CallContext context, FlightStream flightStream,
                    StreamListener<PutResult> ackStream) {
                return () -> QueueAppender.handleIncomingData(appenderThreadLocal.get(), flightStream, ackStream);
            }
            @Override
            public void getStream(CallContext context, Ticket ticket,
                                  ServerStreamListener listener) {
                System.out.println("    Flight Producer: getStream called for ticket: " +
                        new String(ticket.getBytes(), StandardCharsets.UTF_8));
                listener.completed();
            }  
            
             @Override
            public void listFlights(CallContext context, Criteria criteria,
                                    StreamListener<FlightInfo> listener) {
                System.out.println("    Flight Producer: listFlights called");
                listener.onCompleted();
            }

            @Override
            public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
                System.out.println("    Flight Producer: getFlightInfo called");
                return null;
            }            
            @Override
            public void doAction(CallContext context, Action action,
                                 StreamListener<Result> listener) {
                System.out.println("    Flight Producer: doAction called - " + action.getType());
                listener.onCompleted();
            }
            @Override
            public void listActions(CallContext context, StreamListener<ActionType> listener) {
                System.out.println("    Flight Producer: listActions called");
                listener.onCompleted();
            }
        };



        FlightServer server=FlightServer.builder(allocator,serverLocation,producer).build();
        return server;

    }

    //Start
    public void start()throws Exception{
        try{
            this.flightServer.start();
        }catch(Exception e){
            throw  new Exception("Error Starting Flight server");
        }
    }





    //Shutdown
    
    public void shutdown(){
        this.isRunning.set(false);
        System.out.println("\n===FLIGHT SERVER SHUTDOWN STARTING ===");
        try{
            flightServer.close();
            if (chronicleQueue != null) {
                chronicleQueue.close();
            }
            if (allocator != null) {
                allocator.close();
            }
            appenderThreadLocal.remove();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
