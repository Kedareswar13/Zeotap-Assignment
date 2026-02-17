package com.zeotap.fanout.core.config;

import java.util.List;

public final class AppConfig {
  public InputConfig input;
  public RuntimeConfig runtime;
  public List<SinkConfig> sinks;
  public DlqConfig dlq;
}
