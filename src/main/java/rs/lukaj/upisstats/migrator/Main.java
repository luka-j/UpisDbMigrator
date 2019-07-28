package rs.lukaj.upisstats.migrator;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class Main {

    public static final boolean LOG = true;
    public static final boolean LOG_DEBUG = false;

    public static final File    DATA_ROOT = new File("/home/luka/Documents/upis/data");
    private static int     dbYear        = 2017;
    private static String  dataYear      = "17";

    public static void main(String[] args) {
        String user = System.getenv("PG_USER");
        String pass = System.getenv("PG_PASSWORD");
        String name = System.getenv("PG_UPISDB");
        if(name == null) name = System.getenv("PG_DBNAME");
        for(String year : args) {
            try {
                System.out.println("Loading data for year " + year);
                setYear(Integer.parseInt(year));
                doMigrate(user, pass, name);
                System.out.println("Finished migration for " + year);
            } catch (NumberFormatException ex) {
                System.err.println("Invalid year: " + year);
            }
        }
        if(args.length == 0) doMigrate(user, pass, name); //if no args are supplied, use default
    }

    private static void doMigrate(String user, String pass, String dbName) {
        try(Migrator migrator = new Migrator(user, pass, dbName, getDbYear())) {
            //todo do evolutions (if tables don't exist)
            migrator.loadToDatabase(getDataYear());
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void setYear(int year) {
        if(year > 2000) {
            dbYear = year;
            dataYear = String.valueOf(year % 2000);
        } else {
            dbYear = year + 2000;
            dataYear = String.valueOf(year);
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

    public static int getDbYear() {
        return dbYear;
    }

    public static String getDataYear() {
        return dataYear;
    }
}
