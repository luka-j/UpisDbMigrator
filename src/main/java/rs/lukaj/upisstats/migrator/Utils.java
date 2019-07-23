package rs.lukaj.upisstats.migrator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

    private static final Map<Character, String> stripGuillemets = new HashMap<>();
    static {
        stripGuillemets.put('š', "s");
        stripGuillemets.put('đ', "dj");
        stripGuillemets.put('č', "c");
        stripGuillemets.put('ć', "c");
        stripGuillemets.put('ž', "z");

    }
    public static String stripSpecialChars(String text) {
        StringBuilder basicString = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            String str;
            if(Character.isUpperCase(c)) c = Character.toLowerCase(c);
            if(!Character.isLetterOrDigit(c)) continue;
            if(stripGuillemets.containsKey(c)) str = stripGuillemets.get(c);
            else                               str = Character.toString(c);
            basicString.append(str);
        }
        return basicString.toString();
    }
}
