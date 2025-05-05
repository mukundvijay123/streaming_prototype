package org.example;

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
public class SqlToRel {
    public static void main(String[] args) {
        String sql = "SELECT s.id, s.name FROM employees as e , employees as s WHERE s.salary > 30";


        try {
            // Create the root schema and register the table with upper-case name "EMPLOYEES"
            SchemaPlus rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("EMPLOYEES", new EmployeesTable());
            System.out.println(rootSchema);
            FrameworkConfig config = Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema)
                    .parserConfig(org.apache.calcite.sql.parser.SqlParser.Config.DEFAULT.withCaseSensitive(false))
                    .build();

            // Create a single Planner instance to process the query end-to-end.
            Planner planner = Frameworks.getPlanner(config);

            // Parse, validate, and convert the SQL using the same Planner instance.
            SqlNode parsedNode = planner.parse(sql);
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
        } catch (SqlParseException e) {
            System.err.println("Failed to parse SQL: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error during validation or conversion: " + e.getMessage());
        }
    }

    // Table Definition
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