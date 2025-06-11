package serviceb.wsServer;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;
import java.io.StringReader;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;

@ServerEndpoint("/queryEndpoint")
public class QueryEndpoint {
    private String sessionName = null;

    @OnOpen
    public void onOpen(Session session) {
        System.out.println("[WebSocket] Connection opened: " + session.getId());
    }

    @OnMessage
    public void onMessage(String message, Session session){
        try (JsonReader jsonReader = Json.createReader(new StringReader(message))) {
            JsonObject jsonMessage = jsonReader.readObject();
            System.out.println(jsonMessage);

            String action = jsonMessage.getString("action", null);
            System.out.println("Action: " + action);

            if(action=="start_query_session"){

            }else if(action=="delete_quert_session"){

            }else{
                
            }

            //If action is createQuery execute action

            //If action is delete session Delete it here


            

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
    }
}
