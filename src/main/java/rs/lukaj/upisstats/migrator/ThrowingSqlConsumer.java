package rs.lukaj.upisstats.migrator;

import java.sql.SQLException;
import java.util.function.Consumer;

@FunctionalInterface
public interface ThrowingSqlConsumer<T> extends Consumer<T> {
    @Override
    default void accept(T o) {
        try {
            acceptThrows(o);
        } catch (SQLException e) {
            Main.log("SQL exception: " + e.getMessage());
            throw new UncheckedSQLException(e);
        }
    }

    void acceptThrows(T o) throws SQLException;
}
