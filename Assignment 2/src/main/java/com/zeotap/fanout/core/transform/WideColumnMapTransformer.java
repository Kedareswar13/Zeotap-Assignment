package com.zeotap.fanout.core.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zeotap.fanout.core.model.Record;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public final class WideColumnMapTransformer implements Transformer {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public TransformedPayload transform(Record record) throws Exception {
    Map<String, Object> cqlMap = new LinkedHashMap<>();
    cqlMap.put("pk", record.fields().getOrDefault("id", String.valueOf(record.lineNumber())));
    cqlMap.put("columns", record.fields());
    byte[] bytes = MAPPER.writeValueAsBytes(cqlMap);
    return new TransformedPayload("application/cql-map+json", bytes);
  }
}
