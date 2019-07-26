package rs.lukaj.upisstats.migrator;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

//a lame excuse for proper profiling
public class Profiler {
    private static Map<String, Long> times = new HashMap<>();

    public static void addTime(String name, long time) {
        times.put(name, times.getOrDefault(name, 0L)+time);
    }

    public static void printTimes() {
        if(Main.LOG_DEBUG) {
            times.entrySet().stream()
                    .sorted(Collections.reverseOrder(Comparator.comparingLong(Map.Entry::getValue)))
                    .forEach(System.out::println);
        }
    }
}
