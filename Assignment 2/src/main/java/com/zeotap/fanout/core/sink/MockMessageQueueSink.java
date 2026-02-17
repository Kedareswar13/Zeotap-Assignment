package com.zeotap.fanout.core.sink;

import com.zeotap.fanout.core.model.Record;
import com.zeotap.fanout.core.transform.TransformedPayload;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class MockMessageQueueSink implements Sink {
  private final String name;
  private final String endpoint;
  private final double failureProbability;

  public MockMessageQueueSink(String name, String endpoint, double failureProbability) {
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
    Thread.sleep(ThreadLocalRandom.current().nextInt(1, 5));
    if (ThreadLocalRandom.current().nextDouble() < failureProbability) {
      throw new RuntimeException("MQ sink publish failed for endpoint=" + endpoint);
    }
  }
}
