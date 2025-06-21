package serviceb.wsServer;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;
import serviceb.Querying.QueryMetadata;
import serviceb.utils.context;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.api.Context;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonString;

@ServerEndpoint("/queryEndpoint")
public class QueryEndpoint {
    private String sessionName = null;
    private static QueryMetadata queryMetadata;
    private String querySessionName;

    public static void setQueryMetadata(QueryMetadata metadata){
        queryMetadata=metadata;
    } 

    @OnOpen
    public void onOpen(Session session) {
        this.sessionName=session.getId();
        System.out.println("[WebSocket] Connection opened: " + sessionName);
    }

    @OnMessage
    public void onMessage(String message, Session session){
        try (JsonReader jsonReader = Json.createReader(new StringReader(message))) {
            JsonObject jsonMessage = jsonReader.readObject();
            System.out.println(jsonMessage);

            String action = jsonMessage.getString("action", null);
            String token=jsonMessage.getString("token",null);
            context ctx=new context(token);
            System.out.println("Action: " + action);

            if(action.equals("start_query_session")){
                String QueryString=jsonMessage.getString("query_string");
                
                Set<String> topicSet = jsonMessage.getJsonArray("topics")
                    .getValuesAs(JsonString.class)
                    .stream()
                    .map(JsonString::getString)
                    .collect(Collectors.toSet());
                List<String>topics=new ArrayList<>(topicSet);
                try{
                    this.querySessionName=QueryEndpoint.queryMetadata.createQuerySession(QueryString, topics, session,ctx);
                }catch(Exception e){
                    System.err.println("Unable to create query session: "+e.getMessage());
                    e.printStackTrace();
                    //session.close();
                }

            }else if(action.equals("close")){
                try{
                    QueryEndpoint.queryMetadata.deleteQuerySession(this.querySessionName);
                }catch(Exception e){
                    System.err.println("Unable to close query session: "+e.getMessage());
                    session.close();
                }
            }else{
                session.getAsyncRemote().sendText("Invalid input");
            }   

        } catch (Exception e) {
            System.err.println("[WebSocket] Error: " + e.getMessage());
        }
    }

    @OnClose
    public void onClose(Session session) {
        System.out.println("[WebSocket] Connection closed: " + session.getId());
        
    }

    @OnError
    public void onError(Session session, Throwable throwable) {
        System.err.println("[WebSocket] Error on session " + session.getId() + ": " + throwable.getMessage());
        if (session.isOpen()) {
            try {
                session.close();
            } catch (IOException e) {
                System.err.println("Error closing session: " + e.getMessage());
            }
        }
    }
}
