package org.example.rest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ConsumerRepository {

    private final String jdbcUrl;

    public ConsumerRepository(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        createTableIfNotExists();
    }

    private void createTableIfNotExists() {
        String sql = """
                CREATE TABLE IF NOT EXISTS t_consumer (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """;
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Impossible de creer la table t_consumer", e);
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    public ConsumerMessage insert(String message) {
        String sql = "INSERT INTO t_consumer (message) VALUES (?)";
        try (Connection conn = connect();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            stmt.setString(1, message);
            stmt.executeUpdate();
            try (ResultSet keys = stmt.getGeneratedKeys()) {
                keys.next();
                return findById(conn, keys.getLong(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Echec insertion dans t_consumer", e);
        }
    }

    public List<ConsumerMessage> findAll() {
        String sql = "SELECT id, message, created_at FROM t_consumer ORDER BY id";
        List<ConsumerMessage> result = new ArrayList<>();
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                result.add(mapRow(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Echec lecture de t_consumer", e);
        }
        return result;
    }

    private ConsumerMessage findById(Connection conn, long id) throws SQLException {
        String sql = "SELECT id, message, created_at FROM t_consumer WHERE id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return mapRow(rs);
            }
        }
    }

    private ConsumerMessage mapRow(ResultSet rs) throws SQLException {
        return new ConsumerMessage(rs.getLong("id"), rs.getString("message"), rs.getString("created_at"));
    }
}
