package com.zeotap.durable.app;

import com.zeotap.durable.engine.WorkflowRunner;
import com.zeotap.durable.examples.onboarding.CrashConfig;
import com.zeotap.durable.examples.onboarding.EmployeeOnboardingWorkflow;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public final class Main {
  public static void main(String[] args) throws Exception {
    Map<String, String> parsed = parseArgs(args);

    String workflowId = parsed.getOrDefault("--workflowId", "onboarding-001");
    String employeeName = parsed.getOrDefault("--employee", "Alice");
    String crashAt = parsed.get("--crashAt");
    String dbPath = parsed.getOrDefault("--db", "./state.sqlite");
    boolean reset = Boolean.parseBoolean(parsed.getOrDefault("--reset", "false"));
    long zombieTimeoutMs = Long.parseLong(parsed.getOrDefault("--zombieTimeoutMs", "0"));

    Path dbFile = Path.of(dbPath).toAbsolutePath();
    if (dbFile.getParent() != null) {
      Files.createDirectories(dbFile.getParent());
    }

    WorkflowRunner runner = new WorkflowRunner(dbFile.toString(), Duration.ofMillis(zombieTimeoutMs));

    var executor = Executors.newFixedThreadPool(4);
    try {
      System.out.println("Starting/resuming workflowId=" + workflowId + " db=" + dbFile);
      runner.runWithOptions(
          workflowId,
          new EmployeeOnboardingWorkflow(employeeName, executor, new CrashConfig(crashAt)),
          reset,
          Duration.ofMillis(zombieTimeoutMs));
      System.out.println("Workflow completed successfully.");
    } finally {
      executor.shutdownNow();
    }
  }

  private static Map<String, String> parseArgs(String[] args) {
    Map<String, String> out = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      String a = args[i];
      if (a.startsWith("--")) {
        if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
          out.put(a, args[i + 1]);
          i++;
        } else {
          out.put(a, "true");
        }
      }
    }
    return out;
  }
}
