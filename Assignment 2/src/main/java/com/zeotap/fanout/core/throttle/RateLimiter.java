package com.zeotap.fanout.core.throttle;

import java.util.concurrent.locks.LockSupport;

public final class RateLimiter {
  private final long nanosPerPermit;
  private volatile long nextFreeNanos;

  public RateLimiter(int permitsPerSecond) {
    if (permitsPerSecond <= 0) {
      this.nanosPerPermit = 0;
      this.nextFreeNanos = 0;
    } else {
      this.nanosPerPermit = 1_000_000_000L / permitsPerSecond;
      this.nextFreeNanos = System.nanoTime();
    }
  }

  public void acquire() {
    if (nanosPerPermit == 0) {
      return;
    }

    while (true) {
      long now = System.nanoTime();
      long nf = nextFreeNanos;
      long waitNanos = nf - now;
      if (waitNanos <= 0) {
        long newNext = now + nanosPerPermit;
        if (compareAndSetNextFree(nf, newNext)) {
          return;
        }
        continue;
      }
      LockSupport.parkNanos(waitNanos);
    }
  }

  private synchronized boolean compareAndSetNextFree(long expected, long update) {
    if (nextFreeNanos != expected) {
      return false;
    }
    nextFreeNanos = update;
    return true;
  }
}
