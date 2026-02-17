package com.zeotap.fanout.core.obs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public final class Stats {
  private final LongAdder processed = new LongAdder();
  private final Map<String, LongAdder> successBySink = new ConcurrentHashMap<>();
  private final Map<String, LongAdder> failureBySink = new ConcurrentHashMap<>();

  public void incProcessed() {
    processed.increment();
  }

  public long processed() {
    return processed.sum();
  }

  public void incSuccess(String sink) {
    successBySink.computeIfAbsent(sink, k -> new LongAdder()).increment();
  }

  public void incFailure(String sink) {
    failureBySink.computeIfAbsent(sink, k -> new LongAdder()).increment();
  }

  public Map<String, Long> successSnapshot() {
    return snapshot(successBySink);
  }

  public Map<String, Long> failureSnapshot() {
    return snapshot(failureBySink);
  }

  private static Map<String, Long> snapshot(Map<String, LongAdder> src) {
    Map<String, Long> out = new ConcurrentHashMap<>();
    for (var e : src.entrySet()) {
      out.put(e.getKey(), e.getValue().sum());
    }
    return out;
  }
}
