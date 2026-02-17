package com.zeotap.durable.examples.onboarding;

import com.zeotap.durable.engine.DurableContext;
import com.zeotap.durable.engine.Workflow;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public final class EmployeeOnboardingWorkflow implements Workflow {
  private final Activities activities = new Activities();
  private final String employeeName;
  private final ExecutorService executor;
  private final CrashConfig crashConfig;

  public EmployeeOnboardingWorkflow(String employeeName, ExecutorService executor, CrashConfig crashConfig) {
    this.employeeName = employeeName;
    this.executor = executor;
    this.crashConfig = crashConfig;
  }

  @Override
  public void run(DurableContext ctx) throws Exception {
    String employeeId =
        ctx.step(
            "create-record",
            String.class,
            () -> {
              String id = activities.createEmployeeRecord(employeeName);
              crashConfig.maybeCrash("after-create-record");
              return id;
            });

    DurableContext parallel = ctx.scoped("parallel");

    CompletableFuture<String> laptopF =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return parallel.step(
                    "provision-laptop",
                    String.class,
                    () -> {
                      crashConfig.maybeCrash("before-provision-laptop");
                      return activities.provisionLaptop(employeeId);
                    });
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            },
            executor);

    CompletableFuture<String> accessF =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return parallel.step(
                    "provision-access",
                    String.class,
                    () -> {
                      crashConfig.maybeCrash("before-provision-access");
                      return activities.provisionAccess(employeeId);
                    });
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            },
            executor);

    String laptopTicket = laptopF.join();
    String accessTicket = accessF.join();

    ctx.step(
        "send-welcome-email",
        String.class,
        () -> {
          crashConfig.maybeCrash("before-send-welcome-email");
          return activities.sendWelcomeEmail(employeeId) + " (" + laptopTicket + ", " + accessTicket + ")";
        });

    crashConfig.maybeCrash("after-workflow");
  }
}
