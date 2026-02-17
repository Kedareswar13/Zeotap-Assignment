package com.zeotap.durable.engine;

import java.time.Duration;
import java.util.Objects;

public final class WorkflowRunner {
  private final String sqliteFilePath;
  private final Duration zombieTimeout;

  public WorkflowRunner(String sqliteFilePath, Duration zombieTimeout) {
    this.sqliteFilePath = Objects.requireNonNull(sqliteFilePath, "sqliteFilePath");
    this.zombieTimeout = Objects.requireNonNull(zombieTimeout, "zombieTimeout");
  }

  public void run(String workflowId, Workflow workflow) throws Exception {
    Objects.requireNonNull(workflowId, "workflowId");
    Objects.requireNonNull(workflow, "workflow");

    DurableContext ctx = DurableContext.createRoot(workflowId, sqliteFilePath, zombieTimeout);
    try {
      workflow.run(ctx);
    } finally {
      ctx.close();
    }
  }

  public void runWithOptions(String workflowId, Workflow workflow, boolean reset, Duration zombieTimeout)
      throws Exception {
    Objects.requireNonNull(workflowId, "workflowId");
    Objects.requireNonNull(workflow, "workflow");
    Objects.requireNonNull(zombieTimeout, "zombieTimeout");

    DurableContext ctx = DurableContext.createRoot(workflowId, sqliteFilePath, zombieTimeout);
    try {
      if (reset) {
        ctx.resetWorkflowState();
      }
      workflow.run(ctx);
    } finally {
      ctx.close();
    }
  }
}
