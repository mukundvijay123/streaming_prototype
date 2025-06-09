package serviceb;

import serviceb.wsServer.WebsocketServer;

public class App {
  
    public static void main(String[] args) {
        WebsocketServer wsServer = new WebsocketServer();

        // Add shutdown hook for Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received. Stopping WebSocket server...");
            wsServer.stop(); // You'll need to add this method if not already present
        }));

        wsServer.start();
    }

}
