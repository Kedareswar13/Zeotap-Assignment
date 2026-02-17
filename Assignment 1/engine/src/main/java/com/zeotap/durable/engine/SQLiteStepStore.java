package com.zeotap.durable.engine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

final class SQLiteStepStore implements AutoCloseable {
  private final Connection connection;
  private final ReentrantLock dbLock = new ReentrantLock(true);
  private final Duration zombieTimeout;

  SQLiteStepStore(String sqliteFilePath, Duration zombieTimeout) {
    try {
      this.connection = DriverManager.getConnection("jdbc:sqlite:" + sqliteFilePath);
      this.connection.setAutoCommit(true);
      this.zombieTimeout = Objects.requireNonNull(zombieTimeout, "zombieTimeout");
      init();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to open SQLite connection", e);
    }
  }

  Duration zombieTimeout() {
    return zombieTimeout;
  }

  private void init() throws SQLException {
    try (Statement st = connection.createStatement()) {
      st.execute("PRAGMA journal_mode=WAL");
      st.execute("PRAGMA synchronous=NORMAL");
      st.execute("PRAGMA busy_timeout=5000");
    }

    String ddl =
        "CREATE TABLE IF NOT EXISTS steps ("
            + "workflow_id TEXT NOT NULL,"
            + "step_key TEXT NOT NULL,"
            + "status TEXT NOT NULL,"
            + "run_id TEXT NOT NULL,"
            + "output_class TEXT,"
            + "output_json TEXT,"
            + "error TEXT,"
            + "updated_at_ms INTEGER NOT NULL,"
            + "PRIMARY KEY (workflow_id, step_key)"
            + ")";

    try (Statement st = connection.createStatement()) {
      st.execute(ddl);
    }
  }

  Optional<StepRecord> readStep(String workflowId, String stepKey) {
    dbLock.lock();
    try (PreparedStatement ps =
        connection.prepareStatement(
            "SELECT status, run_id, output_class, output_json, error, updated_at_ms FROM steps WHERE workflow_id = ? AND step_key = ?")) {
      ps.setString(1, workflowId);
      ps.setString(2, stepKey);

      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }

        StepStatus status = StepStatus.valueOf(rs.getString(1));
        String runId = rs.getString(2);
        String outputClass = rs.getString(3);
        String outputJson = rs.getString(4);
        String error = rs.getString(5);
        long updatedAt = rs.getLong(6);

        return Optional.of(new StepRecord(status, outputJson, outputClass, error, updatedAt, runId));
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to read step", e);
    } finally {
      dbLock.unlock();
    }
  }

  boolean tryStartStep(String workflowId, String stepKey, String runId) {
    long now = System.currentTimeMillis();

    dbLock.lock();
    try {
      try (PreparedStatement insert =
          connection.prepareStatement(
              "INSERT OR IGNORE INTO steps(workflow_id, step_key, status, run_id, updated_at_ms) VALUES(?, ?, 'RUNNING', ?, ?)")) {
        insert.setString(1, workflowId);
        insert.setString(2, stepKey);
        insert.setString(3, runId);
        insert.setLong(4, now);
        insert.executeUpdate();
      }

      Optional<StepRecord> existing = readStepNoLock(workflowId, stepKey);
      if (existing.isEmpty()) {
        return false;
      }

      StepRecord rec = existing.get();
      if (rec.status == StepStatus.COMPLETED) {
        return false;
      }

      if (rec.status == StepStatus.RUNNING) {
        long ageMs = now - rec.updatedAtEpochMs;
        if (ageMs < zombieTimeout.toMillis()) {
          return false;
        }
      }

      try (PreparedStatement takeover =
          connection.prepareStatement(
              "UPDATE steps SET status='RUNNING', run_id=?, error=NULL, output_class=NULL, output_json=NULL, updated_at_ms=? WHERE workflow_id=? AND step_key=?")) {
        takeover.setString(1, runId);
        takeover.setLong(2, now);
        takeover.setString(3, workflowId);
        takeover.setString(4, stepKey);
        takeover.executeUpdate();
      }

      return true;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to start step", e);
    } finally {
      dbLock.unlock();
    }
  }

  void deleteWorkflow(String workflowId) {
    dbLock.lock();
    try (PreparedStatement ps =
        connection.prepareStatement("DELETE FROM steps WHERE workflow_id = ?")) {
      ps.setString(1, workflowId);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to delete workflow state", e);
    } finally {
      dbLock.unlock();
    }
  }

  void completeStep(String workflowId, String stepKey, String runId, String outputClass, String outputJson) {
    long now = System.currentTimeMillis();

    dbLock.lock();
    try (PreparedStatement ps =
        connection.prepareStatement(
            "UPDATE steps SET status='COMPLETED', output_class=?, output_json=?, error=NULL, updated_at_ms=? WHERE workflow_id=? AND step_key=? AND run_id=?")) {
      ps.setString(1, outputClass);
      ps.setString(2, outputJson);
      ps.setLong(3, now);
      ps.setString(4, workflowId);
      ps.setString(5, stepKey);
      ps.setString(6, runId);

      int updated = ps.executeUpdate();
      if (updated != 1) {
        throw new RuntimeException("Failed to complete step: lost lease or missing record for " + stepKey);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to complete step", e);
    } finally {
      dbLock.unlock();
    }
  }

  void failStep(String workflowId, String stepKey, String runId, String error) {
    long now = System.currentTimeMillis();

    dbLock.lock();
    try (PreparedStatement ps =
        connection.prepareStatement(
            "UPDATE steps SET status='FAILED', error=?, updated_at_ms=? WHERE workflow_id=? AND step_key=? AND run_id=?")) {
      ps.setString(1, error);
      ps.setLong(2, now);
      ps.setString(3, workflowId);
      ps.setString(4, stepKey);
      ps.setString(5, runId);

      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to fail step", e);
    } finally {
      dbLock.unlock();
    }
  }

  private Optional<StepRecord> readStepNoLock(String workflowId, String stepKey) throws SQLException {
    try (PreparedStatement ps =
        connection.prepareStatement(
            "SELECT status, run_id, output_class, output_json, error, updated_at_ms FROM steps WHERE workflow_id = ? AND step_key = ?")) {
      ps.setString(1, workflowId);
      ps.setString(2, stepKey);

      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }

        StepStatus status = StepStatus.valueOf(rs.getString(1));
        String runId = rs.getString(2);
        String outputClass = rs.getString(3);
        String outputJson = rs.getString(4);
        String error = rs.getString(5);
        long updatedAt = rs.getLong(6);

        return Optional.of(new StepRecord(status, outputJson, outputClass, error, updatedAt, runId));
      }
    }
  }

  @Override
  public void close() {
    dbLock.lock();
    try {
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to close connection", e);
    } finally {
      dbLock.unlock();
    }
  }
}
