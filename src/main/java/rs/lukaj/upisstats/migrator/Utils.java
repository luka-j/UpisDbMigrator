package rs.lukaj.upisstats.migrator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    public static String toSnakeCase(String camelCase) {
        Matcher m = Pattern.compile("(?<=[a-z])[A-Z]").matcher(camelCase);
        return m.replaceAll(match -> "_" + match.group().toLowerCase());
    }

    public static int count(int needle, int... haystack) {
        int c = 0;
        for(int a : haystack)
            if(a==needle)
                c++;
        return c;
    }

    public static Map<String, String> extractSchema(ResultSet queryRes) throws SQLException {
        Map<String, String> schema = new HashMap<>();
        while(queryRes.next()) {
            schema.put(queryRes.getString(1), queryRes.getString(2));
        }
        queryRes.close();
        return schema;
    }
}
