package rs.lukaj.upisstats.migrator;

import java.io.IOException;
import java.sql.SQLException;

public class Main {

    public static final boolean LOG = true;
    public static final boolean OVERWRITE_DATA = true;
    public static final int     DB_YEAR        = 2018;
    public static final String  DATA_YEAR      = "18";

    public static void main(String[] args) {
        String user = System.getenv("PG_USER");
        String pass = System.getenv("PG_PASSWORD");
        String name = System.getenv("PG_DBNAME");
        try(Migrator migrator = new Migrator(user, pass, name, DB_YEAR)) {
            //todo do evolutions (if tables don't exist)
            migrator.loadToDatabase(DATA_YEAR);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    public static void log(String msg) {
        if(!LOG) return;
        System.err.println(msg);
    }
}
