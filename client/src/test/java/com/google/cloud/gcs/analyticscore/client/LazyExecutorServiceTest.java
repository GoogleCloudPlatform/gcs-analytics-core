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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
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

  private Callable<String> createCallableTask() {
    return () -> {
      executed.set(true);
      return "success";
    };
  }

  private Runnable createRunnableTask() {
    return () -> executed.set(true);
  }

  @Test
  void submitCallable_isLazyAndRunsOnCallerThread() throws Exception {
    AtomicReference<Thread> executionThread = new AtomicReference<>();
    Callable<String> task =
        () -> {
          executed.set(true);
          executionThread.set(Thread.currentThread());
          return "success";
        };

    Future<String> future = executorService.submit(task);
    assertThat(executed.get()).isFalse();
    String result = future.get();

    assertThat(result).isEqualTo("success");
    assertThat(executed.get()).isTrue();
    assertThat(executionThread.get()).isEqualTo(Thread.currentThread());
  }

  @Test
  void submitRunnable_isLazy() throws Exception {
    Future<?> future = executorService.submit(createRunnableTask());

    assertThat(executed.get()).isFalse();
    future.get();

    assertThat(executed.get()).isTrue();
  }

  @Test
  void shutdown_throwsCancellationExceptionOnGet() {
    Future<String> future = executorService.submit(createCallableTask());

    executorService.shutdown();

    assertThat(executorService.isShutdown()).isTrue();
    assertThrows(CancellationException.class, future::get);
    assertThat(executed.get()).isFalse();
    assertThat(future.isCancelled()).isTrue();
    assertThat(future.isDone()).isTrue();
  }

  @Test
  void shutdownNow_returnsEmptyListAndCancelsFutureTaskExecution() {
    Future<String> future = executorService.submit(createCallableTask());

    java.util.List<Runnable> unexecutedTasks = executorService.shutdownNow();

    assertThat(unexecutedTasks).isEmpty();
    assertThat(executorService.isShutdown()).isTrue();
    assertThrows(CancellationException.class, future::get);
    assertThat(executed.get()).isFalse();
    assertThat(future.isCancelled()).isTrue();
    assertThat(future.isDone()).isTrue();
  }

  @Test
  void submitCallable_isLazyWithTimeout() throws Exception {
    Future<String> future = executorService.submit(createCallableTask());

    assertThat(executed.get()).isFalse();
    String result = future.get(10, java.util.concurrent.TimeUnit.SECONDS);

    assertThat(result).isEqualTo("success");
    assertThat(executed.get()).isTrue();
  }

  @Test
  void submitRunnable_isLazyWithTimeout() throws Exception {
    Future<?> future = executorService.submit(createRunnableTask());

    assertThat(executed.get()).isFalse();
    future.get(10, java.util.concurrent.TimeUnit.SECONDS);

    assertThat(executed.get()).isTrue();
  }

  @Test
  void shutdown_throwsCancellationExceptionOnGetWithTimeout() {
    Future<String> future = executorService.submit(createCallableTask());

    executorService.shutdown();

    assertThrows(
        CancellationException.class, () -> future.get(10, java.util.concurrent.TimeUnit.SECONDS));
    assertThat(executed.get()).isFalse();
    assertThat(future.isCancelled()).isTrue();
    assertThat(future.isDone()).isTrue();
  }

  @Test
  void awaitTermination_andIsTerminated() throws Exception {
    assertThat(executorService.isTerminated()).isFalse();
    executorService.shutdown();

    boolean terminated =
        executorService.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);

    assertThat(executorService.isTerminated()).isTrue();
    assertThat(terminated).isTrue();
  }

  @Test
  void execute_throwsRejectedExecutionException() {
    assertThrows(
        java.util.concurrent.RejectedExecutionException.class,
        () -> executorService.execute(() -> {}));
  }

  @Test
  void completedTask_returnsResultAfterShutdown() throws Exception {
    Future<String> future = executorService.submit(createCallableTask());
    future.get();

    executorService.shutdown();

    assertThat(future.get()).isEqualTo("success");
    assertThat(future.get(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("success");
  }

  @Test
  void submitRunnable_withResult() throws Exception {
    Future<String> future = executorService.submit(createRunnableTask(), "success");

    assertThat(executed.get()).isFalse();
    String result = future.get();

    assertThat(result).isEqualTo("success");
    assertThat(executed.get()).isTrue();
  }

  @Test
  void submitNullTask_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> executorService.submit((Callable<String>) null));
  }

  @Test
  void submitAfterShutdown_throwsRejectedExecutionException() {
    executorService.shutdown();

    assertThrows(
        java.util.concurrent.RejectedExecutionException.class,
        () -> executorService.submit(() -> "task"));
  }
}
