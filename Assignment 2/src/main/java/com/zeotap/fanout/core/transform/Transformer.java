package com.zeotap.fanout.core.transform;

import com.zeotap.fanout.core.model.Record;

public interface Transformer {
  TransformedPayload transform(Record record) throws Exception;
}
