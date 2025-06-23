package serviceb.Querying;


import serviceb.arrowio.arrowOutputTransform;
import serviceb.arrowio.outputMessage;

public class QueryResultBroadcasterWorker implements Runnable {
    private final QueryMetadata metadata;

    public QueryResultBroadcasterWorker(QueryMetadata metadata){
        this.metadata=metadata;
    }

    @Override
    public void run(){
        while(true){
            try{
                outputMessage message=arrowOutputTransform.outputQueue.take();
                String sessionName=message.querySession;
                QueryCtx ctx=metadata.fetchCtx(sessionName);
                if(ctx!=null){
                    ctx.sendText(message.outputRow);
                }
                                
            }catch(Exception e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }

}
