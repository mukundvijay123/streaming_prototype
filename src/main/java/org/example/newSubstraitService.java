package org.example;


import io.substrait.proto.Plan;
import io.substrait.isthmus.SqlToSubstrait;
import com.google.common.collect.ImmutableList;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;


public class newSubstraitService {
    private static final int PORT = 8080;

    // Global hashmap to store topic -> CREATE TABLE statement mappings
    private final Map<String, String> tableDefinitions = new HashMap<>();
    private final SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();

    public static void main(String[] args) throws Exception {
        newSubstraitService service = new newSubstraitService();
        service.startServer();
    }

    public void startServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);

        // Set up the endpoints - create contexts with proper routing logic
        server.createContext("/create", new CreateHandler());
        server.createContext("/alter", new AlterHandler());
        server.createContext("/getSubstrait", new GetSubstraitHandler());
        server.createContext("/delete", new DeleteHandler());

        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("Server started on port " + PORT);
    }

    // Add this method to set CORS headers
    private void setCorsHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type, Authorization");
        exchange.getResponseHeaders().add("Access-Control-Max-Age", "3600");
    }

    // Handler for /create endpoint
    class CreateHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            System.out.println("Received request to /create with method: " + exchange.getRequestMethod());

            // Handle OPTIONS request (preflight)
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if (!"POST".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                sendResponse(exchange, 405, "{\"error\": \"Method Not Allowed\"}");
                return;
            }

            setCorsHeaders(exchange);

            try {
                // Print request headers for debugging
                System.out.println("Processing POST request to /create");
                exchange.getRequestHeaders().forEach((k, v) -> System.out.println(k + ": " + v));

                // Parse request body
                String requestBody = new String(exchange.getRequestBody().readAllBytes());
                System.out.println("Request body: " + requestBody);
                /*
                {
                topic:-,
                createTableStatement:-
                }
                */

                // Extract data from JSON
                String topic = extractValueFromJson(requestBody, "topic");
                String createTableStatement = extractValueFromJson(requestBody, "createTableStatement");

                System.out.println("Extracted topic: " + topic);
                System.out.println("Extracted createTableStatement: " + createTableStatement);

                // Validate inputs
                if (topic.isEmpty() || createTableStatement.isEmpty()) {
                    System.out.println("Invalid input: topic or createTableStatement is empty");
                    sendResponse(exchange, 400, "{\"error\": \"Topic and CREATE TABLE statement are required\"}");
                    return;
                }

                // Add to the global map
                tableDefinitions.put(topic, createTableStatement);
                System.out.println("Added to map: " + topic + " -> " + createTableStatement);
                System.out.println(tableDefinitions);
                sendResponse(exchange, 200, "{\"status\": \"success\", \"message\": \"Created table definition for topic: " + topic + "\"}");
            } catch (Exception e) {
                System.out.println("Error in create handler: " + e.getMessage());
                e.printStackTrace();
                sendResponse(exchange, 400, "{\"error\": \"Error creating table definition: " + e.getMessage() + "\"}");
            }
        }
    }

    // Handler for /alter endpoint
    class AlterHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            System.out.println("Received request to /alter with method: " + exchange.getRequestMethod());

            // Handle OPTIONS request (preflight)
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if (!"POST".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                sendResponse(exchange, 405, "{\"error\": \"Method Not Allowed\"}");
                return;
            }

            setCorsHeaders(exchange);

            try {
                // Parse request body
                String requestBody = new String(exchange.getRequestBody().readAllBytes());
                System.out.println("Request body: " + requestBody);

                String topic = extractValueFromJson(requestBody, "topic");
                String createTableStatement = extractValueFromJson(requestBody, "createTableStatement");

                if (!tableDefinitions.containsKey(topic)) {
                    sendResponse(exchange, 404, "{\"error\": \"Topic not found: " + topic + "\"}");
                    return;
                }

                // Update the existing entry
                tableDefinitions.put(topic, createTableStatement);
                System.out.println("Updated map: " + topic + " -> " + createTableStatement);
                System.out.println(tableDefinitions);
                sendResponse(exchange, 200, "{\"status\": \"success\", \"message\": \"Altered table definition for topic: " + topic + "\"}");
            } catch (Exception e) {
                System.out.println("Error in alter handler: " + e.getMessage());
                e.printStackTrace();
                sendResponse(exchange, 400, "{\"error\": \"Error altering table definition: " + e.getMessage() + "\"}");
            }
        }
    }

    // Handler for /getSubstrait endpoint
    class GetSubstraitHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            System.out.println("Received request to /getSubstrait with method: " + exchange.getRequestMethod());

            // Handle OPTIONS request (preflight)
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if (!"POST".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                sendResponse(exchange, 405, "{\"error\": \"Method Not Allowed\"}");
                return;
            }

            setCorsHeaders(exchange);

            try {
                // Parse request body
                String requestBody = new String(exchange.getRequestBody().readAllBytes());
                System.out.println("Request body: " + requestBody);

                String query = extractValueFromJson(requestBody, "query");
                System.out.println("Extracted query: " + query);
                /*

                map -> {
                topic1: ddl1,
                topic2: ddl2
                }
                 */
                // Validate inputs
                if (query.isEmpty()) {
                    System.out.println("Invalid input: query is empty");
                    sendResponse(exchange, 400, "{\"error\": \"SQL query is required\"}");
                    return;
                }

                // Convert table definitions map values to a list
                List<String> tableStatements = new ArrayList<>(tableDefinitions.values());
                System.out.println("Using table definitions: " + String.join("\n", tableStatements));

                // First validate the query using plannedSQL
//                String validatedSql = plannedSQL(query, tableStatements);
//
//                if (validatedSql.isEmpty()) {
//                    sendResponse(exchange, 400, "{\"error\": \"Invalid SQL query\"}");
//                    return;
//                }

                // Execute SqlToSubstrait with all table definitions
                Plan plan = sqlToSubstrait.execute(query, ImmutableList.copyOf(tableStatements));
                String jsonOutput = TextToJsonParser.parseToJson(plan.toString());
                System.out.println("Generated Substrait plan");
                System.out.println(tableDefinitions);
                // Return the JSON plan
                sendResponse(exchange, 200, jsonOutput);
            } catch (Exception e) {
                System.out.println("Error in getSubstrait handler: " + e.getMessage());
                e.printStackTrace();
                sendResponse(exchange, 400, "{\"error\": \"Error processing query: " + e.getMessage() + "\"}");
            }
        }
    }

    // Handler for /delete endpoint
    class DeleteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            System.out.println("Received request to /delete with method: " + exchange.getRequestMethod());

            // Handle OPTIONS request (preflight)
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if (!"POST".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                sendResponse(exchange, 405, "{\"error\": \"Method Not Allowed\"}");
                return;
            }

            setCorsHeaders(exchange);

            try {
                // Parse request body
                String requestBody = new String(exchange.getRequestBody().readAllBytes());
                System.out.println("Request body: " + requestBody);

                String topic = extractValueFromJson(requestBody, "topic");
                System.out.println("Extracted topic: " + topic);

                if (topic.isEmpty()) {
                    System.out.println("Invalid input: topic is empty");
                    sendResponse(exchange, 400, "{\"error\": \"Topic is required\"}");
                    return;
                }

                if (!tableDefinitions.containsKey(topic)) {
                    sendResponse(exchange, 404, "{\"error\": \"Topic not found: " + topic + "\"}");
                    return;
                }

                // Remove from the global map
                tableDefinitions.remove(topic);
                System.out.println("Removed from map: " + topic);
                System.out.println(tableDefinitions);
                sendResponse(exchange, 200, "{\"status\": \"success\", \"message\": \"Deleted table definition for topic: " + topic + "\"}");
            } catch (Exception e) {
                System.out.println("Error in delete handler: " + e.getMessage());
                e.printStackTrace();
                sendResponse(exchange, 400, "{\"error\": \"Error deleting table definition: " + e.getMessage() + "\"}");
            }
        }
    }

    // Modified plannedSQL method to accept list of table definition

    // Helper method to extract values from JSON strings
    private String extractValueFromJson(String json, String key) {
        // Very basic JSON parsing - in production use a proper JSON parser
        // First try with quoted values (strings)
        String patternString = "\"" + key + "\"\\s*:\\s*\"([^\"]*)\"";
        java.util.regex.Pattern r = java.util.regex.Pattern.compile(patternString);
        java.util.regex.Matcher m = r.matcher(json);
        if (m.find()) {
            return m.group(1);
        }

        // If not found, try without quotes (numbers, booleans)
        String patternNonString = "\"" + key + "\"\\s*:\\s*([^,}\\s][^,}]*)";
        r = java.util.regex.Pattern.compile(patternNonString);
        m = r.matcher(json);
        if (m.find()) {
            return m.group(1);
        }

        System.out.println("Failed to extract value for key: " + key + " from JSON: " + json);
        return "";
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, response.length());
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }

    // Table definition class
    static class EmployeesTable extends AbstractTable {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("id", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER))
                    .add("name", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR))
                    .add("salary", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER))
                    .build();
        }
    }
}