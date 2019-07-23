package rs.lukaj.upisstats.migrator;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static rs.lukaj.upisstats.migrator.Main.log;

public class StatementPool implements Closeable {
    private Map<String, PreparedStatement> pool = new HashMap<>();
    private Connection conn;

    public StatementPool(Connection conn) {
        this.conn = conn;
    }

    public PreparedStatement get(String sql) throws SQLException {
        PreparedStatement stmt = pool.get(sql);
        if(stmt == null || stmt.isClosed()) {
            pool.put(sql, conn.prepareStatement(sql));
        }
        return pool.get(sql);
    }

    public void executeBatch(String sql) throws SQLException {
        PreparedStatement stmt = pool.get(sql);
        if(stmt == null || stmt.isClosed()) {
            throw new IllegalStateException("Statement doesn't exist or is closed!");
        }
        stmt.executeBatch();
    }

    public int executeBatchesPrefix(String prefix) throws SQLException {
        int batchesExecuted = 0;
        for(Map.Entry<String, PreparedStatement> e : pool.entrySet()) {
            if(e.getKey().startsWith(prefix)) {
                log("Executing " + e.getKey());
                e.getValue().executeBatch();
                batchesExecuted++;
            }
        }
        return batchesExecuted;
    }

    @Override
    public void close() throws IOException {
        for(PreparedStatement stmt : pool.values()) {
            try {
                stmt.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }
}
