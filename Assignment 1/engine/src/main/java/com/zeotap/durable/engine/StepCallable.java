package com.zeotap.durable.engine;

@FunctionalInterface
public interface StepCallable<T> {
  T call() throws Exception;
}
