package com.zeotap.durable.examples.onboarding;

import java.util.UUID;

final class Activities {
  String createEmployeeRecord(String name) {
    sleep(300);
    return "emp_" + UUID.randomUUID();
  }

  String provisionLaptop(String employeeId) {
    sleep(600);
    return "laptop_ticket_" + employeeId;
  }

  String provisionAccess(String employeeId) {
    sleep(600);
    return "access_ticket_" + employeeId;
  }

  String sendWelcomeEmail(String employeeId) {
    sleep(200);
    return "email_sent_" + employeeId;
  }

  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
