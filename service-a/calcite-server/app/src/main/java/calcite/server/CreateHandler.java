package calcite.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class CreateHandler implements HttpHandler {

    private final ConcurrentHashMap<String, String> tableMap;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SqlParser.Config parserConfig = SqlParser.config().withParserFactory(SqlDdlParserImpl.FACTORY).withCaseSensitive(false);
    
    public CreateHandler(ConcurrentHashMap<String,String> tableDefinitions){
        this.tableMap = tableDefinitions;
    }

    @Override
    public void handle(HttpExchange exchange){
        try{
            Utils.setCorsHeaders(exchange);
            switch (exchange.getRequestMethod()) {
                case "OPTIONS":
                    exchange.sendResponseHeaders(204, -1); // No content
                    return;

                case "POST":
                    handleCreate(exchange);
                    return;

                default:
                    Utils.sendJsonResponse(exchange, 405, "{\"error\": \"Method Not Allowed\"}");
                    return;
            }
        } catch(Exception e){
            System.err.println(e.toString());
            try {
                Utils.sendJsonResponse(exchange, 500, "{\"error\": \"Internal Server Error\"}");
            } catch (IOException ex) {
                System.err.println("Failed to send error response: " + ex.toString());
            }
        }
    }

    private void handleCreate(HttpExchange exchange) throws IOException{
        try{
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            Map<String, String> jsonData = objectMapper.readValue(requestBody, new TypeReference<Map<String, String>>(){});
            
            String topic = jsonData.get("topic");
            String createTableStatement = jsonData.get("createTableStatement");

            if (topic == null || topic.isBlank() || createTableStatement == null || createTableStatement.isBlank()) {
                Utils.sendJsonResponse(exchange, 400, "{\"error\": \"Topic and CREATE TABLE statement are required\"}");
                return;
            }
            /*
            SqlParser parser = SqlParser.create(createTableStatement, parserConfig);
            try{
                SqlNode parsedNode = parser.parseStmt();
                System.out.println(parsedNode.getKind());
                System.out.println(parsedNode.getKind().toString());
                if (!parsedNode.getKind().toString().contains("CREATE_TABLE")){
                    Utils.sendJsonResponse(exchange, 400, "{\"error\": \"DDL statements should be of type CREATE\"}");
                    return;
                }
            } catch(Exception e){
                e.printStackTrace();
                Utils.sendJsonResponse(exchange, 400, "{\"error\": \"Error while parsing DDL statement\"}");
                return;
            }
            */

            if(this.tableMap.containsKey(topic)){
                Utils.sendJsonResponse(exchange, 400, "{\"error\": \"This stream name is already taken\"}");
                return;
            }

            this.tableMap.put(topic, createTableStatement);
            System.out.println("Current tableMap: " + this.tableMap); // Print the ConcurrentHashMap
            Utils.sendJsonResponse(exchange, 201, "{\"success\": \"Stream " + topic + " added to the broker\"}");
            return;

        } catch(Exception e){
            e.printStackTrace(); // Optional: log the error
            Utils.sendJsonResponse(exchange, 400, "{\"error\": \"Invalid JSON or request\"}");
        }
    }
}