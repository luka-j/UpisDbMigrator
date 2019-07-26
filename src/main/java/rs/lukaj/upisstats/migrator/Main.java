package rs.lukaj.upisstats.migrator;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class Main {

    public static final boolean LOG = true;
    public static final boolean LOG_DEBUG = false;

    public static final File    DATA_ROOT = new File("/home/luka/Documents/upis/data");
    public static final int     DB_YEAR        = 2019;
    public static final String  DATA_YEAR      = "19";

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

    public static void debug(String msg) {
        if(!LOG_DEBUG) return;
        System.out.println(msg);
    }
}
