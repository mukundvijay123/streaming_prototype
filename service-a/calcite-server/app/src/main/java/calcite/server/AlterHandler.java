package calcite.server;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class AlterHandler implements HttpHandler {

    private final ConcurrentHashMap<String, String> tableMap;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SqlParser.Config parserConfig = SqlParser.config()
            .withParserFactory(SqlDdlParserImpl.FACTORY)
            .withCaseSensitive(false);

    public AlterHandler(ConcurrentHashMap<String, String> tableDefinitions) {
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
                    handleAlter(exchange);
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

    private void handleAlter(HttpExchange exchange) throws IOException {
        try {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            Map<String, String> jsonData = objectMapper.readValue(requestBody, new TypeReference<Map<String, String>>() {});
            
            String topic = jsonData.get("topic");
            String createTableStatement = jsonData.get("createTableStatement");

            if (topic == null || topic.isBlank() || createTableStatement == null || createTableStatement.isBlank()) {
                Utils.sendJsonResponse(exchange, 400, "{\"error\": \"Topic and CREATE TABLE statement are required\"}");
                return;
            }

            // Note: SQL validation is commented out in the original code
            // If you want to enable it, you can uncomment and use the fixed version below
            /*
            try {
                SqlParser parser = SqlParser.create(createTableStatement, parserConfig);
                SqlNode parsedNode = parser.parseStmt();
                System.out.println("Parsed SQL node kind: " + parsedNode.getKind());
                
                if (!parsedNode.getKind().toString().contains("CREATE_TABLE")) {
                    Utils.sendJsonResponse(exchange, 400, "{\"error\": \"DDL statements should be of type CREATE TABLE\"}");
                    return;
                }
            } catch (Exception e) {
                System.err.println("SQL parsing error: " + e.getMessage());
                e.printStackTrace();
                Utils.sendJsonResponse(exchange, 400, "{\"error\": \"Error while parsing DDL statement: " + e.getMessage() + "\"}");
                return;
            }
            */

            if (!this.tableMap.containsKey(topic)) {
                Utils.sendJsonResponse(exchange, 400, "{\"error\": \"This stream doesn't exist. Schema cannot be altered.\"}");
                return;
            }

            this.tableMap.put(topic, createTableStatement);
            Utils.sendJsonResponse(exchange, 200, "{\"success\": \"Stream " + topic + " schema changed\"}");
            return;
        } catch (Exception e) {
            e.printStackTrace();
            Utils.sendJsonResponse(exchange, 400, "{\"error\": \"Invalid JSON or request\"}");
        }
    }
}