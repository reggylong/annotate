import java.io.*;
import java.util.*;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;
import java.lang.*;

class TimeoutRunner implements Runnable {

  ExecutorService pool;
  Runnable runner;
  long timeoutSeconds;

  TimeoutRunner(ExecutorService pool, Runnable runner, long timeoutSeconds) {
    this.pool = pool;
    this.runner = runner;
    this.timeoutSeconds = timeoutSeconds;
  }

  private void endTask(Future future) {
    future.cancel(true); 
  }

  public void run() {
    Future<?> future = pool.submit(runner);
    try {
      future.get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      System.err.println("TimeoutRunner: interrupted");
      endTask(future);
    } catch (ExecutionException e) {
      int failed = Main.failed.incrementAndGet();
      System.err.println("Thread: " + runner + " threw an exception");
      Utils.printFailed(failed);
      endTask(future);
    } catch (TimeoutException e) {
      int failed = Main.failed.incrementAndGet();
      System.err.println("Thread: " + runner + " timed out");
      Utils.printFailed(failed);
      endTask(future);
    }
    int examined = Main.count.incrementAndGet();
    printExamined(examined);
  }

  private void printFailed(int failed) {
    if (failed % 10 == 0) {
      System.out.println(failed + " number of executions have failed.");
    }
  }

  private void printExamined(int examined) {
    if (examined % 10 == 0) {
      long diffTime = System.nanoTime() - Main.startTime;
      System.out.println("[" + TimeUnit.NANOSECONDS.toMinutes(diffTime) +
          " min(s) elapsed]" +
          + examined + " examined");
    }
  }
}
