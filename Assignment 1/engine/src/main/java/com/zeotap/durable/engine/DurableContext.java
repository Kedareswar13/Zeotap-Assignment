package com.zeotap.durable.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class DurableContext {
  private final String workflowId;
  private final SQLiteStepStore stepStore;
  private final ObjectMapper objectMapper;
  private final String runId;
  private final String scopePrefix;
  private final ConcurrentHashMap<String, AtomicInteger> perIdSequence;

  DurableContext(
      String workflowId,
      SQLiteStepStore stepStore,
      ObjectMapper objectMapper,
      String runId,
      String scopePrefix,
      ConcurrentHashMap<String, AtomicInteger> perIdSequence) {
    this.workflowId = Objects.requireNonNull(workflowId, "workflowId");
    this.stepStore = Objects.requireNonNull(stepStore, "stepStore");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    this.runId = Objects.requireNonNull(runId, "runId");
    this.scopePrefix = Objects.requireNonNull(scopePrefix, "scopePrefix");
    this.perIdSequence = Objects.requireNonNull(perIdSequence, "perIdSequence");
  }

  public String workflowId() {
    return workflowId;
  }

  public String runId() {
    return runId;
  }

  public Duration zombieTimeout() {
    return stepStore.zombieTimeout();
  }

  public void resetWorkflowState() {
    stepStore.deleteWorkflow(workflowId);
  }

  public DurableContext scoped(String scope) {
    String normalized = Objects.requireNonNull(scope, "scope").trim();
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("scope must be non-empty");
    }
    String newPrefix = scopePrefix + normalized + "/";
    return new DurableContext(workflowId, stepStore, objectMapper, runId, newPrefix, perIdSequence);
  }

  public <T> T step(String id, Class<T> clazz, StepCallable<T> fn) throws Exception {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(clazz, "clazz");
    Objects.requireNonNull(fn, "fn");

    String fullId = scopePrefix + id;
    int seq = nextSequence(fullId);
    StepKey key = new StepKey(fullId, seq);
    String stepKey = key.keyString();

    Optional<StepRecord> existing = stepStore.readStep(workflowId, stepKey);
    if (existing.isPresent() && existing.get().status == StepStatus.COMPLETED) {
      String json = existing.get().outputJson;
      if (json == null) {
        return null;
      }
      return objectMapper.readValue(json, clazz);
    }

    boolean canRun = stepStore.tryStartStep(workflowId, stepKey, runId);
    if (!canRun) {
      Optional<StepRecord> after = stepStore.readStep(workflowId, stepKey);
      if (after.isPresent() && after.get().status == StepStatus.COMPLETED) {
        String json = after.get().outputJson;
        if (json == null) {
          return null;
        }
        return objectMapper.readValue(json, clazz);
      }

      if (after.isPresent() && after.get().status == StepStatus.RUNNING) {
        throw new IllegalStateException(
            "Step is currently RUNNING and within zombie timeout: " + stepKey);
      }

      if (after.isPresent() && after.get().status == StepStatus.FAILED) {
        throw new IllegalStateException(
            "Step previously FAILED and was not completed: " + stepKey + " error=" + after.get().error);
      }

      throw new IllegalStateException("Step could not be started: " + stepKey);
    }

    try {
      T result = fn.call();
      String json = serialize(result);
      stepStore.completeStep(workflowId, stepKey, runId, clazz.getName(), json);
      return result;
    } catch (Exception e) {
      stepStore.failStep(workflowId, stepKey, runId, e.toString());
      throw e;
    }
  }

  public <T> T step(String id, StepCallable<T> fn) throws Exception {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(fn, "fn");

    String fullId = scopePrefix + id;
    int seq = nextSequence(fullId);
    StepKey key = new StepKey(fullId, seq);
    String stepKey = key.keyString();

    Optional<StepRecord> existing = stepStore.readStep(workflowId, stepKey);
    if (existing.isPresent() && existing.get().status == StepStatus.COMPLETED) {
      String json = existing.get().outputJson;
      String className = existing.get().outputClass;
      if (json == null) {
        return null;
      }
      if (className == null || className.isBlank() || className.equals(Void.class.getName())) {
        return null;
      }
      @SuppressWarnings("unchecked")
      Class<T> clazz = (Class<T>) Class.forName(className);
      return objectMapper.readValue(json, clazz);
    }

    boolean canRun = stepStore.tryStartStep(workflowId, stepKey, runId);
    if (!canRun) {
      Optional<StepRecord> after = stepStore.readStep(workflowId, stepKey);
      if (after.isPresent() && after.get().status == StepStatus.COMPLETED) {
        String json = after.get().outputJson;
        String className = after.get().outputClass;
        if (json == null) {
          return null;
        }
        if (className == null || className.isBlank() || className.equals(Void.class.getName())) {
          return null;
        }
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) Class.forName(className);
        return objectMapper.readValue(json, clazz);
      }

      if (after.isPresent() && after.get().status == StepStatus.RUNNING) {
        throw new IllegalStateException(
            "Step is currently RUNNING and within zombie timeout: " + stepKey);
      }

      if (after.isPresent() && after.get().status == StepStatus.FAILED) {
        throw new IllegalStateException(
            "Step previously FAILED and was not completed: " + stepKey + " error=" + after.get().error);
      }

      throw new IllegalStateException("Step could not be started: " + stepKey);
    }

    try {
      T result = fn.call();
      String outputClass = (result == null) ? Void.class.getName() : result.getClass().getName();
      String json = serialize(result);
      stepStore.completeStep(workflowId, stepKey, runId, outputClass, json);
      return result;
    } catch (Exception e) {
      stepStore.failStep(workflowId, stepKey, runId, e.toString());
      throw e;
    }
  }

  private int nextSequence(String fullId) {
    AtomicInteger seq = perIdSequence.computeIfAbsent(fullId, ignored -> new AtomicInteger(0));
    return seq.getAndIncrement();
  }

  private String serialize(Object obj) {
    if (obj == null) {
      return null;
    }
    try {
      return objectMapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize step output", e);
    }
  }

  static DurableContext createRoot(String workflowId, String sqliteFilePath, Duration zombieTimeout) {
    SQLiteStepStore store = new SQLiteStepStore(sqliteFilePath, zombieTimeout);
    ObjectMapper mapper = new ObjectMapper();
    String runId = UUID.randomUUID().toString();
    return new DurableContext(workflowId, store, mapper, runId, "", new ConcurrentHashMap<>());
  }

  void close() {
    stepStore.close();
  }
}
