package com.zeotap.fanout.core.transform;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.zeotap.fanout.core.model.Record;
import java.util.Map;

public final class ProtobufTransformer implements Transformer {
  @Override
  public TransformedPayload transform(Record record) {
    Struct.Builder b = Struct.newBuilder();
    for (Map.Entry<String, Object> e : record.fields().entrySet()) {
      b.putFields(e.getKey(), toValue(e.getValue()));
    }
    byte[] bytes = b.build().toByteArray();
    return new TransformedPayload("application/x-protobuf", bytes);
  }

  private static Value toValue(Object v) {
    if (v == null) {
      return Value.newBuilder().setNullValueValue(0).build();
    }
    if (v instanceof Number n) {
      return Value.newBuilder().setNumberValue(n.doubleValue()).build();
    }
    if (v instanceof Boolean b) {
      return Value.newBuilder().setBoolValue(b).build();
    }
    return Value.newBuilder().setStringValue(String.valueOf(v)).build();
  }
}
