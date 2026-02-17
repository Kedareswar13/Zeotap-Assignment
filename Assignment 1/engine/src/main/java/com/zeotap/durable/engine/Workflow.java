package com.zeotap.durable.engine;

@FunctionalInterface
public interface Workflow {
  void run(DurableContext ctx) throws Exception;
}
