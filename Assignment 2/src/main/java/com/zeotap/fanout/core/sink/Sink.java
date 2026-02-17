package com.zeotap.fanout.core.sink;

import com.zeotap.fanout.core.model.Record;
import com.zeotap.fanout.core.transform.TransformedPayload;

public interface Sink {
  String name();

  void send(Record record, TransformedPayload payload) throws Exception;
}
