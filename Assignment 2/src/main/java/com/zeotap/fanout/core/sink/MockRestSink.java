package com.zeotap.fanout.core.sink;

import com.zeotap.fanout.core.model.Record;
import com.zeotap.fanout.core.transform.TransformedPayload;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class MockRestSink implements Sink {
  private final String name;
  private final String endpoint;
  private final double failureProbability;

  public MockRestSink(String name, String endpoint, double failureProbability) {
    this.name = Objects.requireNonNull(name, "name");
    this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
    this.failureProbability = failureProbability;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void send(Record record, TransformedPayload payload) throws Exception {
    Thread.sleep(ThreadLocalRandom.current().nextInt(5, 20));
    if (ThreadLocalRandom.current().nextDouble() < failureProbability) {
      throw new RuntimeException("REST sink failed for endpoint=" + endpoint);
    }
  }
}
