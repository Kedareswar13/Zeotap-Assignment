package com.zeotap.fanout.core.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zeotap.fanout.core.model.Record;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class DlqWriter implements AutoCloseable {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final BufferedWriter writer;
  private final Object lock = new Object();

  public DlqWriter(Path path) {
    try {
      Objects.requireNonNull(path, "path");
      if (path.getParent() != null) {
        Files.createDirectories(path.getParent());
      }
      this.writer =
          Files.newBufferedWriter(
              path,
              StandardCharsets.UTF_8,
              StandardOpenOption.CREATE,
              StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new RuntimeException("Failed to open DLQ: " + path.toAbsolutePath(), e);
    }
  }

  public void write(String sinkName, Record record, int attempts, String error) {
    Map<String, Object> obj = new LinkedHashMap<>();
    obj.put("sink", sinkName);
    obj.put("line", record.lineNumber());
    obj.put("attempts", attempts);
    obj.put("error", error);
    obj.put("record", record.fields());

    try {
      String line = MAPPER.writeValueAsString(obj);
      synchronized (lock) {
        writer.write(line);
        writer.newLine();
        writer.flush();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to write DLQ", e);
    }
  }

  @Override
  public void close() throws Exception {
    synchronized (lock) {
      writer.close();
    }
  }
}
