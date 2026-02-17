package com.zeotap.fanout.core.sink;

import com.zeotap.fanout.core.transform.Transformer;
import java.util.Objects;

public final class SinkBundle {
  private final Sink sink;
  private final Transformer transformer;
  private final int rateLimitPerSecond;

  public SinkBundle(Sink sink, Transformer transformer, int rateLimitPerSecond) {
    this.sink = Objects.requireNonNull(sink, "sink");
    this.transformer = Objects.requireNonNull(transformer, "transformer");
    this.rateLimitPerSecond = rateLimitPerSecond;
  }

  public Sink sink() {
    return sink;
  }

  public Transformer transformer() {
    return transformer;
  }

  public int rateLimitPerSecond() {
    return rateLimitPerSecond;
  }
}
