package com.zeotap.fanout.core.ingest;

import com.zeotap.fanout.core.model.Record;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public final class CsvRecordReader implements RecordReader {
  private final BufferedReader reader;
  private final String[] headers;
  private long lineNo;

  public CsvRecordReader(Path path) {
    try {
      this.reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
      String headerLine = reader.readLine();
      if (headerLine == null) {
        throw new IllegalArgumentException("CSV file is empty: " + path);
      }
      this.headers = splitCsvLine(headerLine);
      this.lineNo = 1;
    } catch (IOException e) {
      throw new RuntimeException("Failed to open CSV file: " + path.toAbsolutePath(), e);
    }
  }

  @Override
  public Record next() throws IOException {
    String line = reader.readLine();
    if (line == null) {
      return null;
    }
    lineNo++;
    String[] values = splitCsvLine(line);
    Map<String, Object> fields = new LinkedHashMap<>();
    for (int i = 0; i < headers.length; i++) {
      String key = headers[i];
      String value = (i < values.length) ? values[i] : "";
      fields.put(key, value);
    }
    return new Record(lineNo, fields);
  }

  private static String[] splitCsvLine(String line) {
    return line.split(",", -1);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
