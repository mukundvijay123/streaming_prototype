package calcite.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.protobuf.util.JsonFormat;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.proto.Plan;

public class GetSubstraitPlanHandler implements HttpHandler {
    private final ConcurrentHashMap<String, String> tableMap;
    private final ObjectMapper objectMapper = new ObjectMapper();    

    public GetSubstraitPlanHandler(ConcurrentHashMap<String,String> tableDefinitions){
        this.tableMap = tableDefinitions;
    }

    @Override
    public void handle(HttpExchange exchange) {
        try {
            Utils.setCorsHeaders(exchange);
            switch (exchange.getRequestMethod()) {
                case "OPTIONS":
                    exchange.sendResponseHeaders(204, -1); // No content
                    break;
                case "POST":
                    handleGetPlan(exchange);
                    break;
                default:
                    Utils.sendJsonResponse(exchange, 405, "{\"error\": \"Method Not Allowed\"}");
                    break;
            }
        } catch (Exception e) {
            try {
                Utils.sendJsonResponse(exchange, 500, "{\"error\": \"Internal Server Error: " + e.getMessage() + "\"}");
            } catch (IOException ioe) {
                System.err.println("Failed to send error response: " + ioe.toString());
            }
            System.err.println("Error handling request: " + e.toString());
        }
    }

    private void handleGetPlan(HttpExchange exchange) throws IOException {
        System.out.println("Processing Substrait plan request");
        
        try {
            String requestBody = new String(exchange.getRequestBody().readAllBytes());
            Map<String, String> jsonData = objectMapper.readValue(requestBody, new TypeReference<Map<String, String>>(){});
            String query = jsonData.get("query");

            if (query == null || query.isEmpty()) {
                Utils.sendJsonResponse(exchange, 400, "{\"error\": \"SQL query is required\"}");
                return;
            }

            List<String> tableStatements = new ArrayList<>(this.tableMap.values());
            SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
            
            try {
                Plan plan = sqlToSubstrait.execute(query, tableStatements);
                String jsonPlan = JsonFormat.printer()
                    .includingDefaultValueFields()
                    .preservingProtoFieldNames()
                    .print(plan);
                Utils.sendJsonResponse(exchange, 200, jsonPlan);
            } catch (Exception e) {
                System.err.println("Error creating Substrait plan: " + e.toString());
                Utils.sendJsonResponse(exchange, 400, "{\"error\": \"Failed to create Substrait plan: " + 
                    e.getMessage().replace("\"", "\\\"") + "\"}");
            }
        } catch (IOException e) {
            System.err.println("Error parsing request: " + e.toString());
            Utils.sendJsonResponse(exchange, 400, "{\"error\": \"Invalid request format: " + 
                e.getMessage().replace("\"", "\\\"") + "\"}");
        }
    }
}