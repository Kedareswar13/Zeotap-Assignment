package com.zeotap.fanout.core.transform;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.Struct;
import com.zeotap.fanout.core.model.Record;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TransformersTest {
  @Test
  void jsonTransformerProducesJson() throws Exception {
    Record r = new Record(1, Map.of("id", "1", "name", "Alice"));
    TransformedPayload p = new JsonTransformer().transform(r);
    assertEquals("application/json", p.contentType());
    String s = new String(p.bytes(), StandardCharsets.UTF_8);
    assertTrue(s.contains("Alice"));
  }

  @Test
  void protobufTransformerProducesParsableStruct() throws Exception {
    Record r = new Record(1, Map.of("id", 1, "name", "Alice"));
    TransformedPayload p = new ProtobufTransformer().transform(r);
    assertEquals("application/x-protobuf", p.contentType());
    Struct parsed = Struct.parseFrom(p.bytes());
    assertEquals("Alice", parsed.getFieldsOrThrow("name").getStringValue());
  }

  @Test
  void xmlTransformerProducesXml() throws Exception {
    Record r = new Record(10, Map.of("id", "10", "name", "Alice & Bob"));
    TransformedPayload p = new XmlTransformer().transform(r);
    assertEquals("application/xml", p.contentType());
    String s = new String(p.bytes(), StandardCharsets.UTF_8);
    assertTrue(s.contains("<record"));
    assertTrue(s.contains("Alice &amp; Bob"));
  }

  @Test
  void wideColumnTransformerProducesMapJson() throws Exception {
    Record r = new Record(1, Map.of("id", "1", "name", "Alice"));
    TransformedPayload p = new WideColumnMapTransformer().transform(r);
    assertEquals("application/cql-map+json", p.contentType());
    String s = new String(p.bytes(), StandardCharsets.UTF_8);
    assertTrue(s.contains("\"pk\""));
    assertTrue(s.contains("Alice"));
  }
}
