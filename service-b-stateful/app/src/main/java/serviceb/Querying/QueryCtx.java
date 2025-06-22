    package serviceb.Querying;


    import java.io.IOException;
    import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
    import java.util.Map;
    import java.util.Objects;
    import java.util.concurrent.BlockingQueue;
    import java.util.concurrent.ConcurrentHashMap;
    import java.util.concurrent.LinkedBlockingQueue;

    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.apache.beam.runners.direct.DirectOptions;
    import org.apache.beam.sdk.Pipeline;
    import org.apache.beam.sdk.extensions.sql.SqlTransform;
    import org.apache.beam.sdk.io.Read;
    import org.apache.beam.sdk.options.PipelineOptionsFactory;
    import org.apache.beam.sdk.options.StreamingOptions;
    import org.apache.beam.sdk.transforms.ParDo;
    import org.apache.beam.sdk.values.PCollection;
    import org.apache.beam.sdk.values.PCollectionTuple;
    import org.apache.beam.sdk.values.Row;
    import org.apache.beam.sdk.values.TupleTag;
    import org.apache.beam.sdk.PipelineResult;
    import org.joda.time.Instant;

    import jakarta.websocket.RemoteEndpoint;
    import jakarta.websocket.Session;
    import serviceb.arrowio.Stream;
    import serviceb.arrowio.arrowIO;
    import serviceb.arrowio.arrowIOUtils;
    import serviceb.arrowio.arrowOutputTransform;

    public class QueryCtx {
        private Thread queryThread;
        private static final int pollInterval=10;//10 milliseconds
        public final String QueryName;
        private final String QueryString;
        public Map<String,Schema> Topics;
        public Map<String,BlockingQueue<VectorSchemaRoot>> QueueMap;
        private final Pipeline QueryPipeline;
        private Map<String,PCollection<Row>> InputQueryCollections;
        private PCollectionTuple QueryTuple;
        private PipelineResult pipelineResult;
        private Session websocketConnection;
        private  RemoteEndpoint.Async asyncRemote;



        public QueryCtx(String QueryName,String QueryString,Map<String,Schema> Topics,Session wsConn){
            this.QueryName=QueryName;
            this.Topics=Topics;
            this.QueueMap=new ConcurrentHashMap<>();
            this.InputQueryCollections=new HashMap<>();
            this.QueryString=QueryString;
            this.QueryPipeline=CreatePipeline();
            this.websocketConnection=wsConn;
            this.asyncRemote=this.websocketConnection.getAsyncRemote();
        }

        private Pipeline CreatePipeline(){
        DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
        directRunnerOptions.setTargetParallelism(1);

        // Enable streaming mode
        StreamingOptions streamingOptions = directRunnerOptions.as(StreamingOptions.class);
        streamingOptions.setStreaming(true);
        return Pipeline.create(streamingOptions);

        }

        private void CreateQueueMap(){
            for(String topic: this.Topics.keySet()){
                this.QueueMap.put(topic, new LinkedBlockingQueue<>());
            }
        }
        
        private void createPcollectionsTuple() throws Exception{
            arrowIO.Registry.createContext(QueryName);
            for(String topic :this.Topics.keySet()){
                Stream topicStream=new Stream(topic,Topics.get(topic),QueryCtx.pollInterval,this.QueueMap.get(topic));
                arrowIO.Registry.AddStream(QueryName, topic, topicStream);
                
                org.apache.beam.sdk.schemas.Schema PCollectionSchema =arrowIOUtils.ArrowSchemaConverter(this.Topics.get(topic));
                PCollection<Row> topicPCollection =QueryPipeline.apply(QueryName+topic,
                    Read.from(new arrowIO(topic, topic, QueryName, Instant.now())))
                    .setRowSchema(PCollectionSchema);
                InputQueryCollections.put(topic, topicPCollection);
            }
            List<String> topics = new ArrayList<>(this.Topics.keySet());
            this.QueryTuple=PCollectionTuple.of(new TupleTag<>(topics.get(0)),this.InputQueryCollections.get(topics.get(0)));
            if(InputQueryCollections.size()>1){
                int numTopics =topics.size();
                for(int i=0;i<numTopics;i++){
                    this.   QueryTuple=this.QueryTuple.and(new TupleTag<>(topics.get(i)),this.InputQueryCollections.get(topics.get(i)));
                }
            }
            
        }

        private void applySql(){
            PCollection<Row> sqlResult=this.QueryTuple.apply(SqlTransform.query(this.QueryString));
            org.apache.beam.sdk.schemas.Schema outputSchema=sqlResult.getSchema();
            
            sqlResult.apply("Output",ParDo.of(new arrowOutputTransform(QueryName, pollInterval)))
            .setRowSchema(outputSchema);

        }

        public void startQuery(){
            try{
                CreatePipeline();
                CreateQueueMap();
                createPcollectionsTuple();
                applySql();
                System.out.println("before:"+QueryName);
                this.pipelineResult=this.QueryPipeline.run();
                System.out.println("after"+QueryName);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
            public void startQueryAsync() {
            this.queryThread = new Thread(() -> {
                try {
                    CreateQueueMap();
                    createPcollectionsTuple();
                    applySql();
                    System.out.println("before:" + QueryName);
                    this.pipelineResult = this.QueryPipeline.run();
                    System.out.println("after:" + QueryName);

                    
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "Query-" + QueryName);
            this.queryThread.setDaemon(true);
            this.queryThread.start();
        }


        

        
        public void stopQuery() throws IOException {
            if (this.pipelineResult != null) {
                this.pipelineResult.cancel();
            }
            if (queryThread != null && queryThread.isAlive()) {
                try {
                    queryThread.join(1000); // optional: wait 1s for cleanup
                } catch (InterruptedException ignored) {}
            }
        }

        public void supplyData(String topic,VectorSchemaRoot event)throws Exception{
            BlockingQueue<VectorSchemaRoot> queue=this.QueueMap.get(topic);
            queue.put(event);
        }

        public void sendText(String QueryResultMessage){
            this.asyncRemote.sendText(QueryResultMessage);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            QueryCtx other = (QueryCtx) obj;
            return Objects.equals(this.QueryName, other.QueryName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(QueryName);
        }

    }