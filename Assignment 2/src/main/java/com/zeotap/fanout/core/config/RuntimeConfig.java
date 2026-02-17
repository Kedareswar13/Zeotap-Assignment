package com.zeotap.fanout.core.config;

public final class RuntimeConfig {
  public int workerThreads;
  public int queueCapacityPerSink;
  public int maxRetries;
  public int statusIntervalSeconds;
}
