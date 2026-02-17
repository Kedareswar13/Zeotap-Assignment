package com.zeotap.fanout.core;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zeotap.fanout.core.config.AppConfig;
import com.zeotap.fanout.core.config.DlqConfig;
import com.zeotap.fanout.core.config.InputConfig;
import com.zeotap.fanout.core.config.InputFormat;
import com.zeotap.fanout.core.config.RuntimeConfig;
import com.zeotap.fanout.core.config.SinkConfig;
import com.zeotap.fanout.core.config.SinkType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EngineIntegrationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir Path tmp;

  @Test
  void runsEndToEndAndCreatesDlqFile() throws Exception {
    Path input = tmp.resolve("in.csv");
    Files.writeString(
        input,
        "id,name\n1,Alice\n2,Bob\n",
        StandardCharsets.UTF_8);

    Path dlq = tmp.resolve("dlq.jsonl");

    AppConfig cfg = new AppConfig();
    cfg.input = new InputConfig();
    cfg.input.path = input.toString();
    cfg.input.format = InputFormat.CSV;
    cfg.input.maxInFlight = 1000;

    cfg.runtime = new RuntimeConfig();
    cfg.runtime.workerThreads = 1;
    cfg.runtime.queueCapacityPerSink = 10;
    cfg.runtime.maxRetries = 1;
    cfg.runtime.statusIntervalSeconds = 1;

    SinkConfig rest = new SinkConfig();
    rest.name = "rest";
    rest.type = SinkType.REST;
    rest.endpoint = "https://example";
    rest.rateLimitPerSecond = 1000;
    rest.failureProbability = 1.0; // force failures

    cfg.sinks = List.of(rest);

    cfg.dlq = new DlqConfig();
    cfg.dlq.path = dlq.toString();

    new Engine(cfg).run();

    assertTrue(Files.exists(dlq));
    String content = Files.readString(dlq, StandardCharsets.UTF_8);
    assertTrue(content.contains("\"sink\":\"rest\""));
  }
}
