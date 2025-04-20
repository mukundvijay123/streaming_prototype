package org.example;

import java.util.*;
import java.util.regex.*;

public class TextToJsonParser {

    private static class Node {
        private Map<String, Object> properties = new LinkedHashMap<>();
        private Map<String, List<Object>> repeatedProperties = new LinkedHashMap<>();
        private List<Node> childNodes = new ArrayList<>();
        private String name;

        public Node(String name) {
            this.name = name;
        }

        public void addProperty(String key, Object value) {
            // Check if this is a repeated property
            if (repeatedProperties.containsKey(key)) {
                repeatedProperties.get(key).add(value);
            } else if (properties.containsKey(key)) {
                // Convert to repeated property
                List<Object> values = new ArrayList<>();
                values.add(properties.get(key));
                values.add(value);
                repeatedProperties.put(key, values);
                properties.remove(key);
            } else {
                properties.put(key, value);
            }
        }

        public void addChild(Node child) {
            childNodes.add(child);
        }

        public Map<String, Object> toJson() {
            Map<String, Object> result = new LinkedHashMap<>();

            // First add all simple properties
            result.putAll(properties);

            // Add all repeated properties as arrays
            for (Map.Entry<String, List<Object>> entry : repeatedProperties.entrySet()) {
                result.put(entry.getKey(), entry.getValue());
            }

            // Handle child nodes
            if (!childNodes.isEmpty()) {
                // Group child nodes by name
                Map<String, List<Node>> nodesByName = new LinkedHashMap<>();

                for (Node child : childNodes) {
                    if (!nodesByName.containsKey(child.name)) {
                        nodesByName.put(child.name, new ArrayList<>());
                    }
                    nodesByName.get(child.name).add(child);
                }

                // Convert child nodes to JSON
                for (Map.Entry<String, List<Node>> entry : nodesByName.entrySet()) {
                    String childName = entry.getKey();
                    List<Node> nodes = entry.getValue();

                    if (nodes.size() == 1) {
                        // Single node, add directly
                        result.put(childName, nodes.get(0).toJson());
                    } else {
                        // Multiple nodes with same name, create array
                        List<Map<String, Object>> nodeJsons = new ArrayList<>();
                        for (Node node : nodes) {
                            nodeJsons.add(node.toJson());
                        }
                        result.put(childName, nodeJsons);
                    }
                }
            }

            return result;
        }
    }

    public static String parseToJson(String input) {
        if (input == null || input.trim().isEmpty()) {
            return "{}";
        }

        // Root node to hold all top-level elements
        Node rootNode = new Node("root");

        // Parse the input
        parseNode(input, 0, rootNode);

        // Convert to JSON
        Map<String, Object> jsonObject = rootNode.toJson();

        // Pretty print the JSON
        return prettyPrintJson(jsonObject);
    }

    private static int parseNode(String input, int startPos, Node parentNode) {
        int pos = startPos;
        int length = input.length();

        StringBuilder keyBuilder = new StringBuilder();
        StringBuilder valueBuilder = new StringBuilder();
        Node currentNode = null;

        while (pos < length) {
            char c = input.charAt(pos);

            // Skip whitespace
            if (Character.isWhitespace(c)) {
                pos++;
                continue;
            }

            // Start of a block
            if (c == '{') {
                if (currentNode != null) {
                    pos = parseNode(input, pos + 1, currentNode);
                    parentNode.addChild(currentNode);
                    currentNode = null;
                } else {
                    pos++;
                }
                continue;
            }

            // End of a block
            if (c == '}') {
                return pos + 1;
            }

            // Read key
            keyBuilder.setLength(0);
            while (pos < length && input.charAt(pos) != ':' && input.charAt(pos) != '{' && input.charAt(pos) != '}') {
                keyBuilder.append(input.charAt(pos));
                pos++;
            }

            String key = keyBuilder.toString().trim();

            // If we hit an opening brace, this is a child node
            if (pos < length && input.charAt(pos) == '{') {
                currentNode = new Node(key);
                continue;
            }

            // Skip colon
            if (pos < length && input.charAt(pos) == ':') {
                pos++;
            }

            // Read value
            valueBuilder.setLength(0);
            boolean inQuotes = false;

            while (pos < length) {
                c = input.charAt(pos);

                if (c == '"') {
                    inQuotes = !inQuotes;
                }

                if (!inQuotes && (c == '\n' || c == '{' || c == '}')) {
                    break;
                }

                valueBuilder.append(c);
                pos++;
            }

            String value = valueBuilder.toString().trim();

            // Convert value to appropriate type
            Object convertedValue;

            if (value.startsWith("\"") && value.endsWith("\"")) {
                // String value
                convertedValue = value.substring(1, value.length() - 1);
            } else if (value.matches("^-?\\d+$")) {
                // Integer value
                convertedValue = Integer.parseInt(value);
            } else if (value.matches("^-?\\d+\\.\\d+$")) {
                // Float value
                convertedValue = Double.parseDouble(value);
            } else if (value.equals("true") || value.equals("false")) {
                // Boolean value
                convertedValue = Boolean.parseBoolean(value);
            } else if (value.isEmpty()) {
                // Empty value, skip
                continue;
            } else {
                // Default to string
                convertedValue = value;
            }

            // Add property to parent node
            parentNode.addProperty(key, convertedValue);
        }

        return pos;
    }

    private static String prettyPrintJson(Object obj) {
        return prettyPrintJson(obj, 0);
    }

    private static String prettyPrintJson(Object obj, int indent) {
        StringBuilder sb = new StringBuilder();
        String indentStr = "  ".repeat(indent);

        if (obj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) obj;

            if (map.isEmpty()) {
                sb.append("{}");
            } else {
                sb.append("{\n");

                boolean first = true;
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    if (!first) {
                        sb.append(",\n");
                    }
                    first = false;

                    sb.append(indentStr).append("  \"").append(entry.getKey()).append("\": ");
                    sb.append(prettyPrintJson(entry.getValue(), indent + 1));
                }

                sb.append("\n").append(indentStr).append("}");
            }
        } else if (obj instanceof List) {
            List<?> list = (List<?>) obj;

            if (list.isEmpty()) {
                sb.append("[]");
            } else {
                sb.append("[\n");

                boolean first = true;
                for (Object item : list) {
                    if (!first) {
                        sb.append(",\n");
                    }
                    first = false;

                    sb.append(indentStr).append("  ");
                    sb.append(prettyPrintJson(item, indent + 1));
                }

                sb.append("\n").append(indentStr).append("]");
            }
        } else if (obj == null) {
            sb.append("null");
        } else if (obj instanceof String) {
            sb.append("\"").append(escapeJsonString((String) obj)).append("\"");
        } else {
            sb.append(obj.toString());
        }

        return sb.toString();
    }

    private static String escapeJsonString(String input) {
        return input
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
                .replace("\b", "\\b")
                .replace("\f", "\\f");
    }

    public static void main(String[] args) {
        String input = "extension_uris {\n" +
                "  extension_uri_anchor: 1\n" +
                "  uri: \"/functions_comparison.yaml\"\n" +
                "}\n" +
                "extensions {\n" +
                "  extension_function {\n" +
                "    extension_uri_reference: 1\n" +
                "    name: \"gt:any_any\"\n" +
                "  }\n" +
                "}\n" +
                "relations {\n" +
                "  root {\n" +
                "    input {\n" +
                "      project {\n" +
                "        common {\n" +
                "          emit {\n" +
                "            output_mapping: 3\n" +
                "            output_mapping: 4\n" +
                "            output_mapping: 5\n" +
                "          }\n" +
                "        }\n" +
                "        input {\n" +
                "          filter {\n" +
                "            common {\n" +
                "              direct {\n" +
                "              }\n" +
                "            }\n" +
                "            input {\n" +
                "              read {\n" +
                "                common {\n" +
                "                  direct {\n" +
                "                  }\n" +
                "                }\n" +
                "                base_schema {\n" +
                "                  names: \"ID\"\n" +
                "                  names: \"NAME\"\n" +
                "                  names: \"SALARY\"\n" +
                "                  struct {\n" +
                "                    types {\n" +
                "                      i32 {\n" +
                "                        nullability: NULLABILITY_REQUIRED\n" +
                "                      }\n" +
                "                    }\n" +
                "                    types {\n" +
                "                      varchar {\n" +
                "                        length: 100\n" +
                "                        nullability: NULLABILITY_NULLABLE\n" +
                "                      }\n" +
                "                    }\n" +
                "                    types {\n" +
                "                      i32 {\n" +
                "                        nullability: NULLABILITY_REQUIRED\n" +
                "                      }\n" +
                "                    }\n" +
                "                    nullability: NULLABILITY_REQUIRED\n" +
                "                  }\n" +
                "                }\n" +
                "                named_table {\n" +
                "                  names: \"EMPLOYEES\"\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "            condition {\n" +
                "              scalar_function {\n" +
                "                output_type {\n" +
                "                  bool {\n" +
                "                    nullability: NULLABILITY_REQUIRED\n" +
                "                  }\n" +
                "                }\n" +
                "                arguments {\n" +
                "                  value {\n" +
                "                    selection {\n" +
                "                      direct_reference {\n" +
                "                        struct_field {\n" +
                "                          field: 2\n" +
                "                        }\n" +
                "                      }\n" +
                "                      root_reference {\n" +
                "                      }\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "                arguments {\n" +
                "                  value {\n" +
                "                    literal {\n" +
                "                      i32: 50000\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "        expressions {\n" +
                "          selection {\n" +
                "            direct_reference {\n" +
                "              struct_field {\n" +
                "              }\n" +
                "            }\n" +
                "            root_reference {\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "        expressions {\n" +
                "          selection {\n" +
                "            direct_reference {\n" +
                "              struct_field {\n" +
                "                field: 1\n" +
                "              }\n" +
                "            }\n" +
                "            root_reference {\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "        expressions {\n" +
                "          selection {\n" +
                "            direct_reference {\n" +
                "              struct_field {\n" +
                "                field: 2\n" +
                "              }\n" +
                "            }\n" +
                "            root_reference {\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "    names: \"ID\"\n" +
                "    names: \"NAME\"\n" +
                "    names: \"SALARY\"\n" +
                "  }\n" +
                "}\n";

        String json = parseToJson(input);
        System.out.println(json);
    }
}
