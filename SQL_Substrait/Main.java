package org.example;
import org.apache.calcite.*;
import io.substrait.proto.Plan;
import io.substrait.isthmus.SqlToSubstrait;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.example.TextToJsonParser.parseToJson;

public class Main {
    public static void main(String[] args) throws Exception{

        String sql = "SELECT * FROM employees WHERE salary > 50000 ORDER BY NAME DESC";
        String employees_schema = "CREATE TABLE employees (id INT NOT NULL, name VARCHAR(100), salary INT NOT NULL)";
        SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
        Plan plan = sqlToSubstrait.execute(sql, ImmutableList.of(employees_schema));
        System.out.println(plan.toString());
    }
}