package serviceb.Querying;

public class QueryingUtils {
    
    public QueryingUtils(){

    }




    public static String QueryType(String query) {
        if (query == null) return null;

        String upperQuery = query.toUpperCase();

        // Common keywords that indicate stateful behavior
        String[] statefulKeywords = {
            "GROUP BY", "ORDER BY", "JOIN", "OVER", "WINDOW", "PARTITION BY",
            "SUM(", "AVG(", "COUNT(", "MIN(", "MAX(", "DISTINCT"
        };

        for (String keyword : statefulKeywords) {
            if (upperQuery.contains(keyword)) {
                System.out.println("statefulRead");
                return "statefulRead"; // Query uses stateful construct
            }
        }
        System.out.println("statelessRead");
        return "statelessRead"; // No stateful constructs found
    }



}
