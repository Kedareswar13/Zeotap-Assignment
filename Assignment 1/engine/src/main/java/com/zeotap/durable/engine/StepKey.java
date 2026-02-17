package com.zeotap.durable.engine;

import java.util.Objects;

final class StepKey {
  private final String id;
  private final int sequence;

  StepKey(String id, int sequence) {
    this.id = Objects.requireNonNull(id, "id");
    this.sequence = sequence;
  }

  String id() {
    return id;
  }

  int sequence() {
    return sequence;
  }

  String keyString() {
    return id + "#" + sequence;
  }
}
