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

import java.util.Date;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
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


public class SqlQueryService {
    private static final int PORT = 8080;
    private static final String FORWARD_URL = "http://127.0.0.1:8000/run-substrait";
    private static final String EMPLOYEES_SCHEMA = "CREATE TABLE employees (id INT NOT NULL, name VARCHAR(100), salary INT NOT NULL)";

    private final Map<String, String> clientQueries = new HashMap<>();
    private final SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();

    public static void main(String[] args) throws Exception {
        SqlQueryService service = new SqlQueryService();
        service.startServer();
    }

    public void startServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/update", new UpdateHandler());
        server.createContext("/query", new QueryHandler());
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
        System.out.println(port);
        return ipAddress + ":" + port;
    }

    // Add this method to set CORS headers
    private void setCorsHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type, Authorization");
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

            setCorsHeaders(exchange); // Set CORS headers for the actual response

            String clientAddress = getClientAddress(exchange);
            String query = new String(exchange.getRequestBody().readAllBytes());

            // Update the map
            clientQueries.put(clientAddress, query);

            // Process the query
            try {
                String s = plannedSQL(query);
                Plan plan = sqlToSubstrait.execute(s, ImmutableList.of(EMPLOYEES_SCHEMA));
                String jsonOutput = TextToJsonParser.parseToJson(plan.toString());
                System.out.println(jsonOutput);
                // Forward to another service
                forwardToService(clientAddress, s, jsonOutput);

                sendResponse(exchange, 200, "Query updated and processed successfully");
            } catch (Exception e) {
                sendResponse(exchange, 400, "Error processing query: " + e.getMessage());
            }
        }
    }

    public String plannedSQL(String query) throws Exception{
        String s = "";
        try {
            SchemaPlus rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("EMPLOYEES", new SqlToRel.EmployeesTable());
            System.out.println(rootSchema);
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
            String S = sqlNode.toSqlString(dialect, false).getSql();
            System.out.println("Generated SQL:\n" + S);
            System.out.println("Relational Expression: \n" + relNode.explain());
            s = S;
        }
        catch(Exception e){
            s = "";
        }
        return s;
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
                sendResponse(exchange, 404, "No query found for this client");
                return;
            }

            sendResponse(exchange, 200, query);
        }
    }

    private void forwardToService(String clientAddress, String query, String jsonOutput) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(FORWARD_URL).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);

        Date date = new Date();

        System.out.println("DATE IS:" + date);
        // Create a properly formatted JSON object
        // The jsonOutput is already in JSON format, so we don't need to escape it further
        System.out.println(escapeJsonString(clientAddress));
        String requestBody = String.format(
                "{\"clientAddress\":\"%s\",\"query\":\"%s\",\"plan\":%s}",
                escapeJsonString(clientAddress),
                escapeJsonString(query),
                jsonOutput  // This is already JSON, don't wrap in quotes or escape
        );
        System.out.println(requestBody);
        try (OutputStream os = connection.getOutputStream()) {
            os.write(requestBody.getBytes());
            os.flush();
        }

        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            System.err.println("Failed to forward to service. Response code: " + responseCode);
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
        exchange.sendResponseHeaders(statusCode, response.length());
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }

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