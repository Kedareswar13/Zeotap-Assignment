package com.zeotap.fanout.core.config;

public final class SinkConfig {
  public String name;
  public SinkType type;
  public String endpoint;
  public int rateLimitPerSecond;
  public double failureProbability;
}
