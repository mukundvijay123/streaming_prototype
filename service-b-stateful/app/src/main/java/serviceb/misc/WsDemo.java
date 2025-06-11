package serviceb.misc;

import serviceb.Querying.QueryMetadata;
import serviceb.wsServer.WebsocketServer;

public class WsDemo{
  
    public static void main(String[] args) {
        QueryMetadata metadata =null;
        WebsocketServer wsServer = new WebsocketServer(metadata);

        // Add shutdown hook for Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received. Stopping WebSocket server...");
            wsServer.stop(); // You'll need to add this method if not already present
        }));

        wsServer.start();
    }

}
