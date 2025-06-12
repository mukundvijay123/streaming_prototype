package org.example;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import java.util.*;

public class CalciteSqlParser {
    private static SqlParser.Config createParserConfig() {
        return SqlParser.config()
                .withParserFactory(SqlDdlParserImpl.FACTORY)
                .withCaseSensitive(false);
    }
    public static Set<String> extractTableNames(String sql) throws Exception {
        SqlParser parser = SqlParser.create(sql, createParserConfig());
        SqlNode sqlNode = parser.parseQuery();

        TableNameShuttle shuttle = new TableNameShuttle();
        sqlNode.accept(shuttle);

        return shuttle.getTableNames();
    }

    public static Set<String> extractTableNames(String sql, List<String> ddlStatements) throws Exception {
        return extractTableNames(sql);
    }

    static class TableNameShuttle extends SqlShuttle {
        private final Set<String> tableNames = new HashSet<>();

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) call;
                if (select.getFrom() != null) {
                    processFromClause(select.getFrom());
                }
                processWhereClause(select.getWhere());
            }

            return super.visit(call);
        }

        private void processFromClause(SqlNode fromNode) {
            if (fromNode instanceof SqlIdentifier) {
                SqlIdentifier identifier = (SqlIdentifier) fromNode;
                if (identifier.isSimple()) {
                    tableNames.add(identifier.getSimple());
                } else {
                    tableNames.add(identifier.names.get(identifier.names.size() - 1));
                }
            } else if (fromNode instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) fromNode;
                if (call.getOperator().getKind() == SqlKind.AS) {
                    SqlNode table = call.getOperandList().get(0);
                    processFromClause(table);
                } else {
                    for (SqlNode operand : call.getOperandList()) {
                        if (operand != null) {
                            processFromClause(operand);
                        }
                    }
                }
            } else if (fromNode instanceof SqlJoin) {
                SqlJoin join = (SqlJoin) fromNode;
                processFromClause(join.getLeft());
                processFromClause(join.getRight());
            } else if (fromNode instanceof SqlSelect) {
                SqlSelect subQuery = (SqlSelect) fromNode;
                if (subQuery.getFrom() != null) {
                    processFromClause(subQuery.getFrom());
                }
            }
        }

        private void processWhereClause(SqlNode whereNode) {
            if (whereNode == null) return;

            if (whereNode instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) whereNode;
                if (call.getOperator().getKind() == SqlKind.EXISTS) {
                    for (SqlNode operand : call.getOperandList()) {
                        if (operand instanceof SqlSelect) {
                            SqlSelect subQuery = (SqlSelect) operand;
                            if (subQuery.getFrom() != null) {
                                processFromClause(subQuery.getFrom());
                            }
                        }
                    }
                }

                if (call.getOperator().getKind() == SqlKind.IN) {
                    for (SqlNode operand : call.getOperandList()) {
                        if (operand instanceof SqlSelect) {
                            SqlSelect subQuery = (SqlSelect) operand;
                            if (subQuery.getFrom() != null) {
                                processFromClause(subQuery.getFrom());
                            }
                        }
                    }
                }

                for (SqlNode operand : call.getOperandList()) {
                    if (operand != null) {
                        processWhereClause(operand);
                    }
                }
            }
        }

        public Set<String> getTableNames() {
            return tableNames;
        }
    }

    private static boolean isKeyword(String word) {

        return keywords.contains(word.toUpperCase());
    }

    // Example usage
    public static void main(String[] args) {
        try {
            // Test queries
            String[] testQueries = {
                    "SELECT * FROM employees",
                    "(SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id) UNION ALL (SELECT * FROM F)",
                    "SELECT p.name FROM projects p WHERE p.budget > 10000",
                    "SELECT \n" +
                            "    customer_id,\n" +
                            "    COUNT(*) AS total_orders,\n" +
                            "    SUM(order_amount) AS total_spent,\n" +
                            "    MAX(order_amount) AS max_order,\n" +
                            "    MIN(order_amount) AS min_order,\n" +
                            "    AVG(order_amount) AS avg_order,\n" +
                            "    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY order_amount) \n" +
                            "        OVER (PARTITION BY customer_id) AS median_order,\n" +
                            "    RANK() OVER (ORDER BY SUM(order_amount) DESC) AS spending_rank\n" +
                            "FROM orders\n" +
                            "WHERE order_date >= (\n" +
                            "    SELECT MIN(order_date)\n" +
                            "    FROM orders\n" +
                            "    WHERE status = 'SHIPPED'\n" +
                            ")\n" +
                            "GROUP BY customer_id\n" +
                            "HAVING SUM(order_amount) > 1000\n" +
                            "ORDER BY total_spent DESC\n" +
                            "LIMIT 10",
                    "SELECT \n" +
                            "    e.name AS employee_name,\n" +
                            "    d.name AS department_name,\n" +
                            "    m.name AS manager_name,\n" +
                            "    s.total_sales\n" +
                            "FROM \n" +
                            "    employees e\n" +
                            "INNER JOIN departments d ON e.dept_id = d.id\n" +
                            "LEFT JOIN employees m ON e.manager_id = m.id\n" +
                            "INNER JOIN (\n" +
                            "    SELECT emp_id, SUM(sale_amount) AS total_sales\n" +
                            "    FROM sales\n" +
                            "    GROUP BY emp_id\n" +
                            ") s ON e.id = s.emp_id\n" +
                            "WHERE \n" +
                            "    d.location = 'New York'\n" +
                            "    AND s.total_sales > 5000\n" +
                            "ORDER BY \n" +
                            "    s.total_sales DESC\n"
            };

            for (String sql : testQueries) {
                System.out.println("SQL: " + sql);

                try {
                    // Method 1: Using SQL parser
                    Set<String> tablesParser = extractTableNames(sql);
                    System.out.println("Tables (parser): " + tablesParser);
                } catch (Exception e) {
                    System.out.println("Parser error: " + e.getMessage());
                }

                try {
                    // Method 2: Using regex (fallback)
//                    Set<String> tablesRegex = extractTableNamesUsingRegex(sql);
//                    System.out.println("Tables (regex): " + tablesRegex);
                } catch (Exception e) {
                    System.out.println("Regex error: " + e.getMessage());
                }

                System.out.println("---");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
