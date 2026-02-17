package com.zeotap.durable.examples.onboarding;

import java.util.Objects;

public final class CrashConfig {
  private final String crashAt;

  public CrashConfig(String crashAt) {
    this.crashAt = crashAt;
  }

  public void maybeCrash(String point) {
    Objects.requireNonNull(point, "point");
    if (crashAt != null && crashAt.equals(point)) {
      System.err.println("Simulating crash at point: " + point);
      System.exit(2);
    }
  }
}
