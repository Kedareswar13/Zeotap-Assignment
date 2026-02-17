package com.zeotap.fanout.core.transform;

import com.zeotap.fanout.core.model.Record;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class XmlTransformer implements Transformer {
  @Override
  public TransformedPayload transform(Record record) {
    StringBuilder sb = new StringBuilder();
    sb.append("<record line=\"").append(record.lineNumber()).append("\">\n");
    for (Map.Entry<String, Object> e : record.fields().entrySet()) {
      sb.append("  <").append(escapeXml(e.getKey())).append(">")
          .append(escapeXml(String.valueOf(e.getValue())))
          .append("</").append(escapeXml(e.getKey())).append(">\n");
    }
    sb.append("</record>");
    return new TransformedPayload("application/xml", sb.toString().getBytes(StandardCharsets.UTF_8));
  }

  private static String escapeXml(String s) {
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
  }
}
