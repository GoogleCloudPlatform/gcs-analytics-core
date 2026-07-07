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
import static org.junit.Assert.assertThrows;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LazyExecutorServiceTest {

  private LazyExecutorService executorService;
  private AtomicBoolean executed;

  @Before
  public void setUp() {
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
  public void testSubmitCallableIsLazyAndRunsOnCallerThread() throws Exception {
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
  public void testSubmitRunnableIsLazy() throws Exception {
    Future<?> future = executorService.submit(createRunnableTask());
    assertThat(executed.get()).isFalse();

    future.get();

    assertThat(executed.get()).isTrue();
  }

  @Test
  public void testShutdownThrowsCancellationExceptionOnGet() {
    Future<String> future = executorService.submit(createCallableTask());
    executorService.shutdown();

    assertThat(executorService.isShutdown()).isTrue();
    assertThrows(CancellationException.class, future::get);
    assertThat(executed.get()).isFalse();
    assertThat(future.isCancelled()).isTrue();
    assertThat(future.isDone()).isTrue();
  }

  @Test
  public void testShutdownNowReturnsEmptyListAndCancelsFutureTaskExecution() {
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
  public void testSubmitCallableIsLazyWithTimeout() throws Exception {
    Future<String> future = executorService.submit(createCallableTask());
    assertThat(executed.get()).isFalse();

    String result = future.get(10, java.util.concurrent.TimeUnit.SECONDS);

    assertThat(result).isEqualTo("success");
    assertThat(executed.get()).isTrue();
  }

  @Test
  public void testSubmitRunnableIsLazyWithTimeout() throws Exception {
    Future<?> future = executorService.submit(createRunnableTask());
    assertThat(executed.get()).isFalse();

    future.get(10, java.util.concurrent.TimeUnit.SECONDS);

    assertThat(executed.get()).isTrue();
  }

  @Test
  public void testShutdownThrowsCancellationExceptionOnGetWithTimeout() {
    Future<String> future = executorService.submit(createCallableTask());
    executorService.shutdown();

    assertThrows(
        CancellationException.class, () -> future.get(10, java.util.concurrent.TimeUnit.SECONDS));
    assertThat(executed.get()).isFalse();
    assertThat(future.isCancelled()).isTrue();
    assertThat(future.isDone()).isTrue();
  }

  @Test
  public void testAwaitTerminationAndIsTerminated() throws Exception {
    assertThat(executorService.isTerminated()).isFalse();

    executorService.shutdown();
    boolean terminated =
        executorService.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);

    assertThat(executorService.isTerminated()).isTrue();
    assertThat(terminated).isTrue();
  }

  @Test
  public void testExecuteThrowsRejectedExecutionException() {
    assertThrows(
        java.util.concurrent.RejectedExecutionException.class,
        () -> executorService.execute(() -> {}));
  }

  @Test
  public void testCompletedTaskReturnsResultAfterShutdown() throws Exception {
    Future<String> future = executorService.submit(createCallableTask());
    future.get();

    executorService.shutdown();

    assertThat(future.get()).isEqualTo("success");
    assertThat(future.get(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("success");
  }

  @Test
  public void testSubmitRunnableWithResult() throws Exception {
    Future<String> future = executorService.submit(createRunnableTask(), "success");
    assertThat(executed.get()).isFalse();

    String result = future.get();

    assertThat(result).isEqualTo("success");
    assertThat(executed.get()).isTrue();
  }

  @Test
  public void testSubmitNullTaskThrowsNullPointerException() {
    assertThrows(NullPointerException.class, () -> executorService.submit((Callable<String>) null));
  }

  @Test
  public void testSubmitAfterShutdownThrowsRejectedExecutionException() {
    executorService.shutdown();

    assertThrows(
        java.util.concurrent.RejectedExecutionException.class,
        () -> executorService.submit(() -> "task"));
  }
}
