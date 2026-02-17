package com.zeotap.fanout.core.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zeotap.fanout.core.model.Record;

public final class JsonTransformer implements Transformer {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public TransformedPayload transform(Record record) throws Exception {
    byte[] bytes = MAPPER.writeValueAsBytes(record.fields());
    return new TransformedPayload("application/json", bytes);
  }
}
