/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LazyExecutorServiceTest {

  private LazyExecutorService executorService;
  private AtomicBoolean executed;

  @BeforeEach
  void setUp() {
    executorService = new LazyExecutorService();
    executed = new AtomicBoolean(false);
  }

  private String createCallableTask() {
    executed.set(true);
    return "success";
  }

  private void createRunnableTask() {
    executed.set(true);
  }

  /**
   * Tests that submitting a Callable is lazy (does not execute on submit), executes on the caller's
   * thread when get() is invoked, and multiple get() calls only execute the task once (verifying
   * the !isDone() check).
   */
  @Test
  void submitCallable_isLazyAndExecutesOnceOnCallerThread() throws Exception {
    AtomicReference<Thread> executionThread = new AtomicReference<>();
    AtomicInteger executionCount = new AtomicInteger(0);
    Callable<String> task =
        () -> {
          executionCount.incrementAndGet();
          executionThread.set(Thread.currentThread());
          return "success";
        };
    Future<String> future = executorService.submit(task);
    boolean executedBeforeGet = executionCount.get() > 0;

    String result1 = future.get(10, SECONDS);
    String result2 = future.get();

    assertThat(executedBeforeGet).isFalse();
    assertThat(result1).isEqualTo("success");
    assertThat(result2).isEqualTo("success");
    assertThat(executionCount.get()).isEqualTo(1);
    assertThat(executionThread.get()).isEqualTo(Thread.currentThread());
  }

  /**
   * Tests that submitting a Runnable is lazy, and multiple get() calls execute the task exactly
   * once.
   */
  @Test
  void submitRunnable_isLazyAndExecutesOnce() throws Exception {
    Future<?> future = executorService.submit(this::createRunnableTask);
    boolean executedBeforeGet = executed.get();

    future.get();
    future.get(10, SECONDS);

    assertThat(executedBeforeGet).isFalse();
    assertThat(executed.get()).isTrue();
  }

  /**
   * Tests that if the executor is shut down before a task's get() is called, the task is implicitly
   * cancelled and get() throws CancellationException.
   */
  @Test
  void shutdown_throwsCancellationExceptionOnGet() {
    Future<String> future = executorService.submit(this::createCallableTask);

    executorService.shutdown();

    assertThat(executorService.isShutdown()).isTrue();
    assertThrows(CancellationException.class, () -> future.get(10, SECONDS));
    assertThrows(CancellationException.class, future::get);
    assertThat(executed.get()).isFalse();
    assertThat(future.isCancelled()).isTrue();
    assertThat(future.isDone()).isTrue();
  }

  /**
   * Tests that shutdownNow cancels pending futures similarly to shutdown, and returns an empty list
   * since tasks are not queued internally.
   */
  @Test
  void shutdownNow_cancelsTasksAndReturnsEmptyList() {
    Future<String> future = executorService.submit(this::createCallableTask);

    List<Runnable> unexecutedTasks = executorService.shutdownNow();

    assertThat(unexecutedTasks).isEmpty();
    assertThat(executorService.isShutdown()).isTrue();
    assertThrows(CancellationException.class, future::get);
    assertThrows(CancellationException.class, () -> future.get(10, SECONDS));
    assertThat(executed.get()).isFalse();
    assertThat(future.isCancelled()).isTrue();
    assertThat(future.isDone()).isTrue();
  }

  @Test
  void awaitTermination_andIsTerminated() throws Exception {
    assertThat(executorService.isTerminated()).isFalse();
    assertThat(executorService.awaitTermination(10, SECONDS)).isFalse();

    executorService.shutdown();

    boolean terminated = executorService.awaitTermination(10, SECONDS);

    assertThat(executorService.isTerminated()).isTrue();
    assertThat(terminated).isTrue();
  }

  @Test
  void execute_throwsRejectedExecutionException() {
    assertThrows(RejectedExecutionException.class, () -> executorService.execute(() -> {}));
  }

  /**
   * Tests that if a task completes before shutdown, subsequent get() calls still return the
   * successful result instead of throwing CancellationException.
   */
  @Test
  void completedTask_returnsResultAfterShutdown() throws Exception {
    Future<String> future = executorService.submit(this::createCallableTask);
    future.get();

    executorService.shutdown();

    assertThat(future.get()).isEqualTo("success");
    assertThat(future.get(10, SECONDS)).isEqualTo("success");
  }

  @Test
  void submitRunnable_withResult() throws Exception {
    Future<String> future = executorService.submit(this::createRunnableTask, "success");
    boolean executedBeforeGet = executed.get();

    String result1 = future.get();
    String result2 = future.get(10, SECONDS);

    assertThat(executedBeforeGet).isFalse();
    assertThat(result1).isEqualTo("success");
    assertThat(result2).isEqualTo("success");
    assertThat(executed.get()).isTrue();
  }

  @Test
  void submitNullTask_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> executorService.submit((Callable<String>) null));
    assertThrows(NullPointerException.class, () -> executorService.submit((Runnable) null));
    assertThrows(NullPointerException.class, () -> executorService.submit((Runnable) null, "res"));
  }

  @Test
  void submitAfterShutdown_throwsRejectedExecutionException() {
    executorService.shutdown();

    assertThrows(RejectedExecutionException.class, () -> executorService.submit(() -> "task"));
    assertThrows(RejectedExecutionException.class, () -> executorService.submit(() -> {}));
  }

  /**
   * Tests that explicitly cancelling a future prevents it from executing, which is natively handled
   * by FutureTask's state checks when run() is invoked.
   */
  @Test
  void cancel_preventsTaskExecution() {
    Future<String> future = executorService.submit(this::createCallableTask);

    future.cancel(true);

    assertThrows(CancellationException.class, future::get);
    assertThrows(CancellationException.class, () -> future.get(10, SECONDS));
    assertThat(executed.get()).isFalse();
  }

  @Test
  void get_whenThreadInterrupted_throwsInterruptedException() {
    Future<String> future = executorService.submit(this::createCallableTask);
    Thread.currentThread().interrupt();

    assertThrows(InterruptedException.class, future::get);

    // Thread.interrupted() clears the interrupt status.
    assertThat(Thread.interrupted()).isFalse();
    assertThat(executed.get()).isFalse();
  }

  @Test
  void getWithTimeout_whenThreadInterrupted_throwsInterruptedException() {
    Future<String> future = executorService.submit(this::createCallableTask);
    Thread.currentThread().interrupt();

    assertThrows(InterruptedException.class, () -> future.get(10, SECONDS));

    // Thread.interrupted() clears the interrupt status.
    assertThat(Thread.interrupted()).isFalse();
    assertThat(executed.get()).isFalse();
  }

  @Test
  void getWithTimeout_whenTimeoutZeroOrNegative_throwsTimeoutException() {
    Future<String> future = executorService.submit(this::createCallableTask);

    assertThrows(TimeoutException.class, () -> future.get(0, SECONDS));
    assertThrows(TimeoutException.class, () -> future.get(-1, SECONDS));

    assertThat(executed.get()).isFalse();
  }

  @Test
  void invokeMethods_throwUnsupportedOperationException() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> executorService.invokeAll(Collections.emptyList()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> executorService.invokeAll(Collections.emptyList(), 10, SECONDS));
    assertThrows(
        UnsupportedOperationException.class,
        () -> executorService.invokeAny(Collections.emptyList()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> executorService.invokeAny(Collections.emptyList(), 10, SECONDS));
  }
}
