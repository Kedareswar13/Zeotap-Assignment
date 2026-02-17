package com.zeotap.fanout.core;

import com.zeotap.fanout.core.config.AppConfig;
import com.zeotap.fanout.core.config.InputFormat;
import com.zeotap.fanout.core.dlq.DlqWriter;
import com.zeotap.fanout.core.ingest.CsvRecordReader;
import com.zeotap.fanout.core.ingest.JsonlRecordReader;
import com.zeotap.fanout.core.ingest.RecordReader;
import com.zeotap.fanout.core.model.Record;
import com.zeotap.fanout.core.obs.Stats;
import com.zeotap.fanout.core.sink.SinkBundle;
import com.zeotap.fanout.core.sink.SinkFactory;
import com.zeotap.fanout.core.throttle.RateLimiter;
import com.zeotap.fanout.core.transform.TransformedPayload;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

public final class Engine {
  private final AppConfig config;

  public Engine(AppConfig config) {
    this.config = Objects.requireNonNull(config, "config");
  }

  public void run() throws Exception {
    Stats stats = new Stats();

    List<SinkBundle> bundles = new ArrayList<>();
    for (var s : config.sinks) {
      bundles.add(SinkFactory.create(s));
    }

    List<SinkRuntime> runtimes = new ArrayList<>();
    for (SinkBundle b : bundles) {
      BlockingQueue<DispatchItem> q = new ArrayBlockingQueue<>(config.runtime.queueCapacityPerSink);
      RateLimiter rl = new RateLimiter(b.rateLimitPerSecond());
      runtimes.add(new SinkRuntime(b, q, rl, new AtomicLong(0)));
    }

    ScheduledExecutorService metrics = Executors.newSingleThreadScheduledExecutor();
    long startMs = System.currentTimeMillis();
    long[] last = new long[] {0};
    metrics.scheduleAtFixedRate(
        () -> {
          long now = System.currentTimeMillis();
          long processed = stats.processed();
          long delta = processed - last[0];
          last[0] = processed;
          double sec = config.runtime.statusIntervalSeconds;
          double rps = delta / sec;
          System.out.println(
              "[STATUS] processed="
                  + processed
                  + " throughput="
                  + String.format("%.2f", rps)
                  + " rec/s"
                  + " successes="
                  + stats.successSnapshot()
                  + " failures="
                  + stats.failureSnapshot());
        },
        config.runtime.statusIntervalSeconds,
        config.runtime.statusIntervalSeconds,
        TimeUnit.SECONDS);

    try (DlqWriter dlq = new DlqWriter(Path.of(config.dlq.path));
        RecordReader reader = openReader()) {

      var sinkExecutor = Executors.newVirtualThreadPerTaskExecutor();
      try {
        for (SinkRuntime rt : runtimes) {
          int workers = effectiveWorkersPerSink();
          for (int i = 0; i < workers; i++) {
            sinkExecutor.submit(() -> sinkWorker(rt, stats, dlq));
          }
        }

        while (true) {
          Record r = reader.next();
          if (r == null) {
            break;
          }
          stats.incProcessed();

          for (SinkRuntime rt : runtimes) {
            rt.inFlight.incrementAndGet();
            rt.queue.put(new DispatchItem(r, 0));
          }
        }

        for (SinkRuntime rt : runtimes) {
          waitForDrain(rt);
          int poisonCount = effectiveWorkersPerSink();
          for (int i = 0; i < poisonCount; i++) {
            rt.queue.put(DispatchItem.POISON);
          }
        }

        sinkExecutor.shutdown();
        sinkExecutor.awaitTermination(1, TimeUnit.HOURS);

      } finally {
        sinkExecutor.shutdownNow();
      }

    } finally {
      metrics.shutdownNow();
    }

    long elapsedMs = System.currentTimeMillis() - startMs;
    System.out.println("Done. Total processed=" + stats.processed() + " elapsed=" + Duration.ofMillis(elapsedMs));
  }

  private RecordReader openReader() {
    Path path = Path.of(config.input.path);
    if (config.input.format == InputFormat.CSV) {
      return new CsvRecordReader(path);
    }
    if (config.input.format == InputFormat.JSONL) {
      return new JsonlRecordReader(path);
    }
    throw new IllegalArgumentException("Unsupported input format: " + config.input.format);
  }

  private int effectiveWorkersPerSink() {
    if (config.runtime.workerThreads <= 0) {
      return Math.max(1, Runtime.getRuntime().availableProcessors());
    }
    return config.runtime.workerThreads;
  }

  private void sinkWorker(SinkRuntime rt, Stats stats, DlqWriter dlq) {
    while (true) {
      DispatchItem item;
      try {
        item = rt.queue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }

      if (item == DispatchItem.POISON) {
        return;
      }

      try {
        rt.rateLimiter.acquire();
        TransformedPayload payload = rt.bundle.transformer().transform(item.record);
        rt.bundle.sink().send(item.record, payload);
        stats.incSuccess(rt.bundle.sink().name());
        rt.inFlight.decrementAndGet();
      } catch (Exception e) {
        int attempts = item.attempt + 1;
        stats.incFailure(rt.bundle.sink().name());

        if (attempts <= config.runtime.maxRetries) {
          try {
            Thread.sleep(backoffMs(attempts));
            rt.queue.put(new DispatchItem(item.record, attempts));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            dlq.write(rt.bundle.sink().name(), item.record, attempts, e.toString());
            rt.inFlight.decrementAndGet();
            return;
          }
        } else {
          dlq.write(rt.bundle.sink().name(), item.record, attempts, e.toString());
          rt.inFlight.decrementAndGet();
        }
      }
    }
  }

  private static void waitForDrain(SinkRuntime rt) throws InterruptedException {
    while (rt.inFlight.get() > 0) {
      Thread.sleep(10);
    }
  }

  private static long backoffMs(int attempt) {
    return Math.min(2000L, 50L * (1L << Math.min(6, attempt)));
  }

  static final class DispatchItem {
    static final DispatchItem POISON = new DispatchItem(null, -1);

    final Record record;
    final int attempt;

    DispatchItem(Record record, int attempt) {
      this.record = record;
      this.attempt = attempt;
    }
  }

  static final class SinkRuntime {
    final SinkBundle bundle;
    final BlockingQueue<DispatchItem> queue;
    final RateLimiter rateLimiter;
    final AtomicLong inFlight;

    SinkRuntime(
        SinkBundle bundle, BlockingQueue<DispatchItem> queue, RateLimiter rateLimiter, AtomicLong inFlight) {
      this.bundle = bundle;
      this.queue = queue;
      this.rateLimiter = rateLimiter;
      this.inFlight = inFlight;
    }
  }
}
