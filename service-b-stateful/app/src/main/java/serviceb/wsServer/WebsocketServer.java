package serviceb.wsServer;

import org.glassfish.tyrus.server.Server;

public class WebsocketServer {

    private final Server server;
    private final String hostAddress;
    private final int port;

    public WebsocketServer(String hostAddress, int port) {
        this.hostAddress = hostAddress;
        this.port = port;
        this.server = new Server(this.hostAddress, this.port, "/ws", null, QueryEndpoint.class);
    }

    public WebsocketServer() {
        this("localhost", 8765);
    }

    public WebsocketServer(String hostAddress) {
        this(hostAddress, 8765);
    }

    public void start() {
        try {
            server.start();
            System.out.println("WebSocket started at ws://" + hostAddress + ":" + port + "/ws");

            // Block main thread so the server keeps running
            //System.out.println("Press Ctrl+C to stop the server...");
            //Thread.currentThread().join(); // Will be interrupted by shutdown hook

        } catch (Exception e) {
            System.out.println("Server thread interrupted.");
        }
    }

    public void stop() {
        System.out.println("Shutting down WebSocket server...");
        server.stop();
    }
}
