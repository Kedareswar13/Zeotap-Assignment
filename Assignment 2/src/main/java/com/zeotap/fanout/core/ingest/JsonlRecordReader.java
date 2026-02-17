package com.zeotap.fanout.core.ingest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zeotap.fanout.core.model.Record;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public final class JsonlRecordReader implements RecordReader {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP_REF = new TypeReference<>() {};

  private final BufferedReader reader;
  private long lineNo;

  public JsonlRecordReader(Path path) {
    try {
      this.reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
      this.lineNo = 0;
    } catch (IOException e) {
      throw new RuntimeException("Failed to open JSONL file: " + path.toAbsolutePath(), e);
    }
  }

  @Override
  public Record next() throws IOException {
    String line = reader.readLine();
    if (line == null) {
      return null;
    }
    lineNo++;
    Map<String, Object> fields = MAPPER.readValue(line, MAP_REF);
    return new Record(lineNo, new LinkedHashMap<>(fields));
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
