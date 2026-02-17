package com.zeotap.fanout.core.transform;

import java.util.Objects;

public final class TransformedPayload {
  private final String contentType;
  private final byte[] bytes;

  public TransformedPayload(String contentType, byte[] bytes) {
    this.contentType = Objects.requireNonNull(contentType, "contentType");
    this.bytes = Objects.requireNonNull(bytes, "bytes");
  }

  public String contentType() {
    return contentType;
  }

  public byte[] bytes() {
    return bytes;
  }
}
