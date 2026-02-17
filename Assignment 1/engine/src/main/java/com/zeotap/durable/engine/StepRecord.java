package com.zeotap.durable.engine;

final class StepRecord {
  final StepStatus status;
  final String outputJson;
  final String outputClass;
  final String error;
  final long updatedAtEpochMs;
  final String runId;

  StepRecord(
      StepStatus status,
      String outputJson,
      String outputClass,
      String error,
      long updatedAtEpochMs,
      String runId) {
    this.status = status;
    this.outputJson = outputJson;
    this.outputClass = outputClass;
    this.error = error;
    this.updatedAtEpochMs = updatedAtEpochMs;
    this.runId = runId;
  }
}
