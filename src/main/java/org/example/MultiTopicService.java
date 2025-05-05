package org.example;

import org.apache.calcite.*;
import io.substrait.proto.Plan;
import io.substrait.isthmus.SqlToSubstrait;
import com.google.common.collect.ImmutableList;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executors;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.sql.SqlDialect;

//SQL -> -> REL NODE -> SQL QUERY -> SUBSTRAIT
public class MultiTopicService {
    private static final int PORT = 8080;
    private static final String FORWARD_URL = "http://127.0.0.1:8765/run-substrait";
    private static final String EMPLOYEES_SCHEMA = "CREATE TABLE STOCK_PRICES (\"timestamp\" TIMESTAMP NOT NULL, stock_symbol VARCHAR(10) NOT NULL, price NUMERIC(10, 2) NOT NULL, volume INTEGER NOT NULL, bid_price NUMERIC(10, 2) NOT NULL, ask_price NUMERIC(10, 2) NOT NULL, spread NUMERIC(10, 2) NOT NULL );";
//[[QUERY, TOPIC]] -> [[SUBSTRAIT, TOPIC]]
    /*
    CLIENT_ENDPOINT -> {
    TOPIC -> SUBSTTRAIT
    }
    */
    private final Map<String, String> clientQueries = new HashMap<>();
    private final Map<String, List<String>> clientTopics = new HashMap<>();
    private final SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();

    // Available topics
    private final List<String> availableTopics = Arrays.asList("ABC", "LMN", "XYZ");

    // Cache of substrait plans by client address
    private final Map<String, List<SubstraitPlanWithTopic>> clientSubstraitPlans = new HashMap<>();

    // Class to hold a substrait plan and its associated topic
    private static class SubstraitPlanWithTopic {
        final String substraitPlan;
        final String topic;

        SubstraitPlanWithTopic(String substraitPlan, String topic) {
            this.substraitPlan = substraitPlan;
            this.topic = topic;
        }
    }

    public static void main(String[] args) throws Exception {
        MultiTopicService service = new MultiTopicService();
        service.startServer();
    }

    public void startServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/update", new UpdateHandler());
        server.createContext("/query", new QueryHandler());
        server.createContext("/topics", new TopicsHandler());
        server.createContext("/client-topics", new ClientTopicsHandler());
        server.createContext("/substrait-plans", new SubstraitPlansHandler());
        server.createContext("/", new OptionsHandler()); // Handle OPTIONS requests at root level
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("Server started on port " + PORT);
    }

    // Get full client address (IP:Port)
    private String getClientAddress(HttpExchange exchange) {
        // First check if the client sent an identity header
        String clientIdentity = exchange.getRequestHeaders().getFirst("X-Client-Identity");
        if (clientIdentity != null && !clientIdentity.isEmpty()) {
            return clientIdentity;
        }

        // Fall back to the connection information
        InetSocketAddress remoteAddress = exchange.getRemoteAddress();
        String ipAddress = remoteAddress.getAddress().getHostAddress();
        String port = "" + remoteAddress.getPort();
        // Normalize IPv6 localhost for consistency
        if (ipAddress.equals("0:0:0:0:0:0:0:1")) {
            ipAddress = "127.0.0.1";
        }
        return ipAddress + ":" + port;
    }

    // Add this method to set CORS headers
    private void setCorsHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Client-Identity");
        exchange.getResponseHeaders().add("Access-Control-Max-Age", "3600");
    }

    // Add this handler for OPTIONS requests (preflight)
    class OptionsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            setCorsHeaders(exchange);
            exchange.sendResponseHeaders(204, -1);
        }
    }

    class UpdateHandler implements HttpHandler {
        public static List<List<String>> parseNestedList(String input) {
            List<List<String>> outerList = new ArrayList<>();

            // Pattern to match inner lists like: ["...","..."]
            Pattern innerListPattern = Pattern.compile("\\[\\s*\"([^\"]*)\"\\s*,\\s*\"([^\"]*)\"\\s*\\]");
            Matcher matcher = innerListPattern.matcher(input);

            while (matcher.find()) {
                List<String> innerList = new ArrayList<>();
                innerList.add(matcher.group(1));
                innerList.add(matcher.group(2));
                outerList.add(innerList);
            }

            return outerList;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Handle OPTIONS request (preflight)
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if (!"POST".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }

            setCorsHeaders(exchange);
            String clientAddress = getClientAddress(exchange);
            String requestBody = new String(exchange.getRequestBody().readAllBytes());

            try {
                // Parse the request body as JSON array
                System.out.println(requestBody);
                List<List<String>> queryTopicPairs = parseNestedList(requestBody);
                System.out.println(queryTopicPairs);
                if (queryTopicPairs.isEmpty()) {
                    sendResponse(exchange, 400, "{\"status\":\"error\",\"message\":\"No query-topic pairs provided\"}");
                    return;
                }

                // Process each query-topic pair
                List<SubstraitPlanWithTopic> plansList = new ArrayList<>();
                for (List<String> pair : queryTopicPairs) {
                    if (pair.size() != 2) {
                        continue; // Skip invalid pairs
                    }

                    String query = pair.get(0);
                    String topic = pair.get(1);

                    try {
                        String plannedSql;
                        if (!query.isEmpty())
                            plannedSql = plannedSQL(query);
                        else
                            plannedSql = "";
                        Plan plan = sqlToSubstrait.execute(plannedSql, ImmutableList.of(EMPLOYEES_SCHEMA));
                        String jsonOutput = TextToJsonParser.parseToJson(plan.toString());
                        plansList.add(new SubstraitPlanWithTopic(jsonOutput, topic));

                        System.out.println(topic);
                        System.out.println("Created substrait plan for topic: " + topic);
                    } catch (Exception e) {
                        System.err.println("Error creating substrait plan for topic " + topic + ": " + e.getMessage());
                    }
                }

                // Store the plans for this client
                clientSubstraitPlans.put(clientAddress, plansList);

                // Create the response JSON
                StringBuilder responseBuilder = new StringBuilder();
                responseBuilder.append("{\"status\":\"success\",\"clientAddress\":\"")
                        .append(escapeJsonString(clientAddress))
                        .append("\",\"plans\":[");

                for (int i = 0; i < plansList.size(); i++) {
                    if (i > 0) {
                        responseBuilder.append(",");
                    }
                    responseBuilder.append("[")
                            .append(plansList.get(i).substraitPlan) // Already JSON formatted
                            .append(",\"")
                            .append(escapeJsonString(plansList.get(i).topic))
                            .append("\"]");
                }
                responseBuilder.append("]}");
                System.out.println(responseBuilder);
                String responseJson = responseBuilder.toString();

                // Forward all plans to another service with topics
                if (!plansList.isEmpty()) {
                    forwardPlansToService(clientAddress, responseJson);
                }

                sendResponse(exchange, 200, responseJson);

            } catch (Exception e) {
                sendResponse(exchange, 400, "{\"status\":\"error\",\"message\":\"Error processing query: " + escapeJsonString(e.getMessage()) + "\"}");
            }
        }

        private List<List<String>> parseJsonArray(String requestBody) {
            List<List<String>> result = new ArrayList<>();

            try {
                // Remove any whitespace and check if it's a JSON array
                String trimmedBody = requestBody.trim();
                if (trimmedBody.startsWith("[")) {
                    // Simple parsing for the expected format [["query1","topic1"],["query2","topic2"]]
                    // This is a simplified parser - for production use a proper JSON library
                    String content = trimmedBody.substring(1, trimmedBody.length() - 1).trim();
                    if (!content.isEmpty()) {
                        String[] pairs = content.split("\\],\\[");
                        for (String pair : pairs) {
                            pair = pair.replaceAll("^\\[|\\]$", "").trim();
                            String[] parts = pair.split(",");
                            if (parts.length == 2) {
                                List<String> pairList = new ArrayList<>();
                                pairList.add(parts[0].replaceAll("^\"|\"$", "").trim()); // query
                                pairList.add(parts[1].replaceAll("^\"|\"$", "").trim()); // topic
                                result.add(pairList);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error parsing JSON array: " + e.getMessage());
            }

            return result;
        }

        // Parse the request body to extract query and topics
        private Map<String, Object> parseRequestBody(String requestBody) {
            Map<String, Object> result = new HashMap<>();

            try {
                // Check if the body is JSON
                if (requestBody.trim().startsWith("{")) {
                    // Very simple JSON parser - in production use a proper JSON library
                    String cleanBody = requestBody.trim();

                    // Extract query
                    int queryStart = cleanBody.indexOf("\"query\"");
                    if (queryStart >= 0) {
                        int valueStart = cleanBody.indexOf(":", queryStart) + 1;
                        while (valueStart < cleanBody.length() &&
                                (cleanBody.charAt(valueStart) == ' ' || cleanBody.charAt(valueStart) == '\t' ||
                                        cleanBody.charAt(valueStart) == '\n' || cleanBody.charAt(valueStart) == '\r')) {
                            valueStart++;
                        }

                        String value;
                        if (cleanBody.charAt(valueStart) == '"') {
                            // String value
                            int valueEnd = findClosingQuote(cleanBody, valueStart + 1);
                            value = cleanBody.substring(valueStart + 1, valueEnd);
                        } else {
                            // Non-string value (should not happen for query)
                            int valueEnd = cleanBody.indexOf(",", valueStart);
                            if (valueEnd < 0) valueEnd = cleanBody.indexOf("}", valueStart);
                            value = cleanBody.substring(valueStart, valueEnd).trim();
                        }
                        result.put("query", value);
                    }

                    // Extract topics array
                    int topicsStart = cleanBody.indexOf("\"topics\"");
                    if (topicsStart >= 0) {
                        int arrayStart = cleanBody.indexOf("[", topicsStart);
                        int arrayEnd = findClosingBracket(cleanBody, arrayStart);

                        String topicsArray = cleanBody.substring(arrayStart + 1, arrayEnd).trim();
                        List<String> topics = new ArrayList<>();

                        if (!topicsArray.isEmpty()) {
                            int pos = 0;
                            while (pos < topicsArray.length()) {
                                // Skip whitespace
                                while (pos < topicsArray.length() && Character.isWhitespace(topicsArray.charAt(pos))) {
                                    pos++;
                                }

                                if (pos >= topicsArray.length()) break;

                                if (topicsArray.charAt(pos) == '"') {
                                    // String topic
                                    int endPos = findClosingQuote(topicsArray, pos + 1);
                                    topics.add(topicsArray.substring(pos + 1, endPos));
                                    pos = endPos + 1;
                                } else {
                                    // Non-string topic (should not happen)
                                    int endPos = topicsArray.indexOf(",", pos);
                                    if (endPos < 0) endPos = topicsArray.length();
                                    topics.add(topicsArray.substring(pos, endPos).trim());
                                    pos = endPos + 1;
                                }

                                // Skip to after comma
                                while (pos < topicsArray.length() && topicsArray.charAt(pos) != ',') {
                                    pos++;
                                }
                                pos++; // Skip comma
                            }
                        }

                        result.put("topics", topics);
                    } else {
                        result.put("topics", new ArrayList<String>());
                    }
                } else {
                    // Treat as plain SQL query if not JSON
                    result.put("query", requestBody.trim());
                    result.put("topics", new ArrayList<String>());
                }
            } catch (Exception e) {
                System.err.println("Error parsing request body: " + e);
                // Default to treating the entire body as the query
                result.put("query", requestBody);
                result.put("topics", new ArrayList<String>());
            }

            return result;
        }

        private int findClosingQuote(String str, int startPos) {
            for (int i = startPos; i < str.length(); i++) {
                if (str.charAt(i) == '"' && (i == 0 || str.charAt(i-1) != '\\')) {
                    return i;
                }
            }
            return str.length();
        }

        private int findClosingBracket(String str, int startPos) {
            int depth = 1;
            for (int i = startPos + 1; i < str.length(); i++) {
                char c = str.charAt(i);
                if (c == '[') depth++;
                else if (c == ']') {
                    depth--;
                    if (depth == 0) return i;
                }
            }
            return str.length();
        }
    }

    public String plannedSQL(String input) throws Exception {
        String query = input;

        // Check if input looks like JSON (starts with '[' or '{')
        if (input.trim().startsWith("[") || input.trim().startsWith("{")) {
            System.out.println("Input appears to be JSON, attempting to extract SQL query");
            try {
                // If it starts with '[', it might be an array format [query, topic]
                if (input.trim().startsWith("[")) {
                    // Parse the first element which should be the query
                    int firstQuoteIndex = input.indexOf("\"");
                    int lastQuoteIndex = input.indexOf("\"", firstQuoteIndex + 1);
                    if (firstQuoteIndex >= 0 && lastQuoteIndex > firstQuoteIndex) {
                        query = input.substring(firstQuoteIndex + 1, lastQuoteIndex);
                        System.out.println("Extracted query from array format: " + query);
                    }
                }
                // If it starts with '{', it might be a JSON object with a query field
                else if (input.trim().startsWith("{")) {
                    int queryFieldIndex = input.indexOf("\"query\"");
                    if (queryFieldIndex >= 0) {
                        int valueStart = input.indexOf(":", queryFieldIndex) + 1;

                        // Skip whitespace
                        while (valueStart < input.length() &&
                                (input.charAt(valueStart) == ' ' || input.charAt(valueStart) == '\t' ||
                                        input.charAt(valueStart) == '\n' || input.charAt(valueStart) == '\r')) {
                            valueStart++;
                        }

                        if (input.charAt(valueStart) == '"') {
                            // String value
                            int valueEnd = findClosingQuote(input, valueStart + 1);
                            query = input.substring(valueStart + 1, valueEnd);
                            System.out.println("Extracted query from JSON object: " + query);
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error extracting SQL from JSON: " + e.getMessage());
                // Keep the original input as query if extraction fails
            }
        }

        // Now process the extracted query through Calcite
        try {
            SchemaPlus rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("STOCK_PRICES", new StockPricesTable());
            System.out.println("Schema: " + rootSchema);

            FrameworkConfig config = Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema)
                    .parserConfig(org.apache.calcite.sql.parser.SqlParser.Config.DEFAULT.withCaseSensitive(false))
                    .build();

            // Create a single Planner instance to process the query end-to-end.
            Planner planner = Frameworks.getPlanner(config);

            // Parse, validate, and convert the SQL using the same Planner instance.
            SqlNode parsedNode = planner.parse(query);
            System.out.println("Parsed SQL AST: " + parsedNode.toString());

            SqlNode validatedNode = planner.validate(parsedNode);
            RelNode relNode = planner.rel(validatedNode).rel;
            SqlDialect dialect = new SqlDialect(
                    SqlDialect.EMPTY_CONTEXT
                            .withDatabaseProduct(SqlDialect.DatabaseProduct.CALCITE)
                            .withIdentifierQuoteString("\"")
            );
            RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
            RelToSqlConverter.Result result = relToSqlConverter.visitRoot(relNode);
            SqlNode sqlNode = result.asStatement();
            String generatedSql = sqlNode.toSqlString(dialect, false).getSql();
            System.out.println("Generated SQL:\n" + generatedSql);
            System.out.println("Relational Expression: \n" + relNode.explain());
            return generatedSql;
        }
        catch(Exception e){
            System.err.println("Error in plannedSQL: " + e.getMessage());
            return query; // Fall back to original query
        }
    }

    // Helper method to find the closing quote
    private int findClosingQuote(String str, int startPos) {
        for (int i = startPos; i < str.length(); i++) {
            if (str.charAt(i) == '"' && (i == 0 || str.charAt(i-1) != '\\')) {
                return i;
            }
        }
        return str.length();
    }

    class QueryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Handle OPTIONS request (preflight)
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            setCorsHeaders(exchange); // Set CORS headers for the actual response

            String clientAddress = getClientAddress(exchange);
            String query = clientQueries.get(clientAddress);

            if (query == null) {
                sendResponse(exchange, 404, "{\"status\":\"error\",\"message\":\"No query found for this client\"}");
                return;
            }

            sendResponse(exchange, 200, "{\"status\":\"success\",\"query\":\"" + escapeJsonString(query) + "\"}");
        }
    }

    class TopicsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Handle OPTIONS request (preflight)
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            setCorsHeaders(exchange); // Set CORS headers for the actual response

            // Return available topics as JSON
            StringBuilder response = new StringBuilder("{\"status\":\"success\",\"topics\":[");
            for (int i = 0; i < availableTopics.size(); i++) {
                if (i > 0) response.append(",");
                response.append("\"").append(escapeJsonString(availableTopics.get(i))).append("\"");
            }
            response.append("]}");

            sendResponse(exchange, 200, response.toString());
        }
    }

    class ClientTopicsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Handle OPTIONS request (preflight)
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            setCorsHeaders(exchange); // Set CORS headers for the actual response

            String clientAddress = getClientAddress(exchange);
            List<String> topics = clientTopics.get(clientAddress);

            if (topics == null) {
                sendResponse(exchange, 404, "{\"status\":\"error\",\"topics\":[]}");
                return;
            }

            // Return client's subscribed topics as JSON
            StringBuilder response = new StringBuilder("{\"status\":\"success\",\"topics\":[");
            for (int i = 0; i < topics.size(); i++) {
                if (i > 0) response.append(",");
                response.append("\"").append(escapeJsonString(topics.get(i))).append("\"");
            }
            response.append("]}");
            System.out.println(response);
            sendResponse(exchange, 200, response.toString());
        }
    }

    class SubstraitPlansHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Handle OPTIONS request (preflight)
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                setCorsHeaders(exchange);
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            setCorsHeaders(exchange); // Set CORS headers for the actual response

            String clientAddress = getClientAddress(exchange);
            List<SubstraitPlanWithTopic> plans = clientSubstraitPlans.get(clientAddress);

            if (plans == null || plans.isEmpty()) {
                sendResponse(exchange, 404, "{\"status\":\"error\",\"message\":\"No substrait plans found for this client\"}");
                return;
            }

            // Return client's substrait plans as JSON
            StringBuilder response = new StringBuilder();
            response.append("{\"status\":\"success\",\"clientAddress\":\"")
                    .append(escapeJsonString(clientAddress))
                    .append("\",\"plans\":[");

            for (int i = 0; i < plans.size(); i++) {
                if (i > 0) response.append(",");
                response.append("[")
                        .append(plans.get(i).substraitPlan) // Already in JSON format
                        .append(",\"")
                        .append(escapeJsonString(plans.get(i).topic))
                        .append("\"]");
            }

            response.append("]}");
            System.out.println(response);
            sendResponse(exchange, 200, response.toString());
        }
    }

    private void forwardPlansToService(String clientAddress, String jsonPayload) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(FORWARD_URL).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");
        connection.setDoOutput(true);

        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = jsonPayload.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        int responseCode = connection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(connection.getErrorStream(), "utf-8"))) {
                StringBuilder errorResponse = new StringBuilder();
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    errorResponse.append(responseLine.trim());
                }
                System.err.println("Failed to forward to service. Response code: " + responseCode +
                        ", Error: " + errorResponse.toString());
            }
        } else {
            System.out.println("Successfully forwarded plans to service");
        }
    }

    private String escapeJsonString(String input) {
        if (input == null) {
            return "";
        }
        return input.replace("\\", "\\\\")  // Must escape backslashes first
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
                .replace("\b", "\\b")
                .replace("\f", "\\f");
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        byte[] responseBytes = response.getBytes();
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }

    static class StockPricesTable extends AbstractTable {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("id", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER))
                    .add("timestamp", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP))
                    .add("stock_symbol", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR))
                    .add("price", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DECIMAL, 10, 2))
                    .add("volume", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER))
                    .add("bid_price", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DECIMAL, 10, 2))
                    .add("ask_price", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DECIMAL, 10, 2))
                    .add("spread", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DECIMAL, 10, 2))
                    .build();
        }
    }


    // TextToJsonParser class (replace with your implementation
}