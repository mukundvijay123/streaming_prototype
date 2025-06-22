package serviceb.Querying;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import jakarta.websocket.Session;
import serviceb.flightServer.StreamSubscribeUtils;
import serviceb.utils.context;

import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class QueryMetadata {
    private static final String AuthorizationUrl="http://localhost:8081/check";
    private Map<String,QueryCtx> QueryMap;
    private Map<String,Set<QueryCtx>> TopicMap;
    private SystemMetadata systemMetadata;
    private String myAddress;
    private String brokerAddress;
    private int QueryCount;
    private final ReentrantReadWriteLock lock;
    private final Lock readLock;
    private final Lock writeLock;
    private final FlightClient flightClient;
    private final BufferAllocator allocator;

    public QueryMetadata(BufferAllocator allocator,String myAddress,String brokerAddress)throws Exception{
        
        this.QueryMap=new HashMap<>();
        this.TopicMap=new HashMap<>();
        this.systemMetadata=new SystemMetadata();
        this.myAddress=myAddress;
        this.brokerAddress=brokerAddress;
        this.lock=new ReentrantReadWriteLock();
        this.readLock=lock.readLock();
        this.writeLock=lock.writeLock();
        this.allocator=new RootAllocator(Long.MAX_VALUE);
        
        URI uri = new URI(brokerAddress); 
        Location location = Location.forGrpcInsecure(uri.getHost(), uri.getPort());
        this.flightClient=FlightClient.builder().allocator(allocator).location(location).build();

        

    }

    private String createQueryName(){
        this.writeLock.lock();
        try{
            this.QueryCount++;
            return "QuerySession"+this.QueryCount;
        }finally{
            this.writeLock.unlock();
        }
    }

    private boolean checkAccess(String token, List<String> Topics, String action) {

        // Filter only the topics present in systemMetadata
        List<String> allowedTopics = Topics.stream()
            .filter(this.systemMetadata::contains)
            .collect(Collectors.toList());

        // If none of the topics are present, return true (nothing to deny)
        if (allowedTopics.isEmpty()) {
            return true;
        }

        // Prepare async access check calls
        List<CompletableFuture<Boolean>> futures = allowedTopics.stream()
            .map(topic -> StreamSubscribeUtils.checkAccessAsync(AuthorizationUrl, token, topic, action))
            .collect(Collectors.toList());

        // Wait for all to complete, then check if all returned true
        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            all.join();  // Waits for all futures to complete
            return futures.stream().allMatch(CompletableFuture::join);
        } catch (Exception e) {
            System.out.println("Error during access checks: " + e.getMessage());
            return false;
        }
    }
    
    private void subscribeToTopics(List<String> topics,context ctx)throws Exception{
        System.out.println(topics);
        for(String topic:topics){
            if(!this.systemMetadata.contains(topic)){
                systemMetadata.add(topic);
                StreamSubscribeUtils.subscribeToTopic(flightClient, myAddress, topic,ctx);
            }
        }
    }

    private void unsubscribeToTopics(List<String> topics){
        for(String topic:topics){
            if(!this.TopicMap.get(topic).isEmpty()){
                systemMetadata.remove(topic);
                StreamSubscribeUtils.unsubscribeToTopic(flightClient, myAddress, topic);
            }
        }
    }

    public QueryCtx fetchCtx(String sessionName){
        this.readLock.lock();
        try{
            return this.QueryMap.get(sessionName);
        }finally{
            this.readLock.unlock();
        }
    }

    public void supplyData(String topic,VectorSchemaRoot table)throws Exception{
        Set<QueryCtx> subscriberContextSet;
        this.readLock.lock();
        try{
            //System.err.println(this.TopicMap.get(topic));
            Set<QueryCtx> tempSet=this.TopicMap.get(topic);
            if(tempSet!=null){
                subscriberContextSet=new HashSet<>(this.TopicMap.get(topic));
            }else{
                subscriberContextSet=new HashSet<>();
            }
            
        }finally{
            this.readLock.unlock();
        }


        for(QueryCtx ctx:subscriberContextSet){
            ctx.supplyData(topic, table);
        }
    }

    public String createQuerySession(String QueryString,List<String> Topics,Session wsconn,context ctx)throws Exception{
        String queryName=createQueryName();
        Map<String,Schema> TopicsSchemaMap=new HashMap<>();
        boolean allowedLocal=checkAccess(ctx.JWTToken, Topics, ctx.action);
        if(!allowedLocal){
            wsconn.getAsyncRemote().sendText("Error Subscribing to topic");
            return null;
        }
        for(String Topic:Topics){
            Schema schema=StreamSubscribeUtils.fetchSchema(Topic, this.flightClient);
            TopicsSchemaMap.put(Topic, schema);
        }
        QueryCtx context =new QueryCtx(queryName, QueryString, TopicsSchemaMap,wsconn);


        this.writeLock.lock();
        try{
            this.QueryMap.put(queryName, context);
            for(String topic:Topics){
                if(TopicMap.containsKey(topic)){
                    TopicMap.get(topic).add(context);
                }else{
                    Set<QueryCtx> newSet=new HashSet<>();
                    newSet.add(context);
                    TopicMap.put(topic, newSet);
                }
            }

        }finally{
            this.writeLock.unlock();
        }

        //releasing the lock in the middle so that write lock isnt held for very long continuosly
        this.writeLock.lock();
        try{
            this.subscribeToTopics(Topics,ctx);
        }catch(Exception e){
            if(e.getMessage().contains("Subscription failed for topic")){
                context.sendText("Failed to subscribe to necessary streams, you are not authorized");
                deleteQuerySession(queryName);
            }
            System.out.println(e.getMessage());
        }finally{
            this.writeLock.unlock();
        }
        context.startQueryAsync();
        return queryName;
    }

    public void deleteQuerySession(String QueryName)throws Exception{
        QueryCtx ctx=this.QueryMap.get(QueryName);
        if(ctx==null){
            return;
        }
        this.writeLock.lock();
        try{

             ctx=this.QueryMap.get(QueryName);
            for(String Topic:ctx.Topics.keySet()){
                this.TopicMap.get(Topic).remove(ctx);
            }
            this.unsubscribeToTopics(new ArrayList<>(ctx.Topics.keySet()));
            ctx.stopQuery();
            this.QueryMap.remove(QueryName);
        }finally{
            this.writeLock.unlock();
        }

        this.writeLock.lock();
        
    }

    public void deleteQueryMetadata()throws Exception{
            this.writeLock.lock();
            allocator.close();
            flightClient.close();
            for(QueryCtx ctx:QueryMap.values()){
                ctx.stopQuery();
            }
            QueryMap.clear();
            TopicMap.clear();
            
        
    }
}
