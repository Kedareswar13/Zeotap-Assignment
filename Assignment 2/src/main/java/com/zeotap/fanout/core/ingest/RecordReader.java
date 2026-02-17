package com.zeotap.fanout.core.ingest;

import com.zeotap.fanout.core.model.Record;
import java.io.Closeable;

public interface RecordReader extends Closeable {
  Record next() throws Exception;
}
