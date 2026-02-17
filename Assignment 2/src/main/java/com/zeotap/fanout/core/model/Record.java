package com.zeotap.fanout.core.model;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class Record {
  private final long lineNumber;
  private final Map<String, Object> fields;

  public Record(long lineNumber, Map<String, Object> fields) {
    this.lineNumber = lineNumber;
    this.fields = Collections.unmodifiableMap(Objects.requireNonNull(fields, "fields"));
  }

  public long lineNumber() {
    return lineNumber;
  }

  public Map<String, Object> fields() {
    return fields;
  }
}
