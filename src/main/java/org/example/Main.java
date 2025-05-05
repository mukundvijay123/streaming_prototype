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

        String sql = "SELECT * FROM STOCK_PRICES";

        String employees_schema = "CREATE TABLE STOCK_PRICES ( id varchar(20) NOT NULL, \"timestamp\" TIMESTAMP NOT NULL, stock_symbol VARCHAR(10) NOT NULL, price NUMERIC(10, 2) NOT NULL, volume INTEGER NOT NULL, bid_price NUMERIC(10, 2) NOT NULL, ask_price NUMERIC(10, 2) NOT NULL, spread NUMERIC(10, 2) NOT NULL );";
        SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
        Plan plan = sqlToSubstrait.execute(sql, ImmutableList.of(employees_schema));
        System.out.println(plan.toString());
    }
}