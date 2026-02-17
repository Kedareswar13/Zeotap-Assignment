package com.zeotap.fanout.core.sink;

import com.zeotap.fanout.core.config.SinkConfig;
import com.zeotap.fanout.core.config.SinkType;
import com.zeotap.fanout.core.transform.JsonTransformer;
import com.zeotap.fanout.core.transform.ProtobufTransformer;
import com.zeotap.fanout.core.transform.Transformer;
import com.zeotap.fanout.core.transform.WideColumnMapTransformer;
import com.zeotap.fanout.core.transform.XmlTransformer;

public final class SinkFactory {
  private SinkFactory() {}

  public static SinkBundle create(SinkConfig cfg) {
    Sink sink;
    Transformer transformer;

    if (cfg.type == SinkType.REST) {
      sink = new MockRestSink(cfg.name, cfg.endpoint, cfg.failureProbability);
      transformer = new JsonTransformer();
    } else if (cfg.type == SinkType.GRPC) {
      sink = new MockGrpcSink(cfg.name, cfg.endpoint, cfg.failureProbability);
      transformer = new ProtobufTransformer();
    } else if (cfg.type == SinkType.MQ) {
      sink = new MockMessageQueueSink(cfg.name, cfg.endpoint, cfg.failureProbability);
      transformer = new XmlTransformer();
    } else if (cfg.type == SinkType.WIDE_COLUMN) {
      sink = new MockWideColumnDbSink(cfg.name, cfg.endpoint, cfg.failureProbability);
      transformer = new WideColumnMapTransformer();
    } else {
      throw new IllegalArgumentException("Unknown sink type: " + cfg.type);
    }

    return new SinkBundle(sink, transformer, cfg.rateLimitPerSecond);
  }
}
