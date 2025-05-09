package calcite.server;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class DeleteHandler implements HttpHandler {
    private final ConcurrentHashMap<String, String> tableMap;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DeleteHandler(ConcurrentHashMap<String, String> tableDefinitions) {
        this.tableMap = tableDefinitions;
    }

    @Override
    public void handle(HttpExchange exchange) {
        try {
            Utils.setCorsHeaders(exchange);

            switch (exchange.getRequestMethod()) {
                case "OPTIONS":
                    exchange.sendResponseHeaders(204, -1); // No content
                    return;
                case "POST":
                    handleDelete(exchange);
                    return;
                default:
                    Utils.sendJsonResponse(exchange, 405, "{\"error\": \"Method Not Allowed\"}");
                    return;
            }
        } catch (Exception e) {
            System.err.println("Error handling request: " + e.toString());
            try {
                Utils.sendJsonResponse(exchange, 500, "{\"error\": \"Internal Server Error\"}");
            } catch (IOException ex) {
                System.err.println("Failed to send error response: " + ex.toString());
            }
        }
    }

    private void handleDelete(HttpExchange exchange) throws IOException {
        try {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            Map<String, String> jsonData = objectMapper.readValue(requestBody, new TypeReference<Map<String, String>>() {});
            String topic = jsonData.get("topic");

            if (topic == null || topic.isBlank()) {
                Utils.sendJsonResponse(exchange, 400, "{\"error\": \"Invalid stream name\"}");
                return;
            }

            if (!this.tableMap.containsKey(topic)) {
                Utils.sendJsonResponse(exchange, 400, "{\"error\": \"This stream doesn't exist. Cannot delete.\"}");
                return;
            }
            
            this.tableMap.remove(topic);
            Utils.sendJsonResponse(exchange, 200, "{\"success\": \"Stream deleted successfully\"}");
            return;
        } catch (Exception e) {
            e.printStackTrace();
            Utils.sendJsonResponse(exchange, 400, "{\"error\": \"Invalid JSON or request\"}");
        }
    }
}