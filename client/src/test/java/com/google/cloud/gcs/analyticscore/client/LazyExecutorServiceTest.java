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

  @Before
  public void setUp() {
    executorService = new LazyExecutorService();
  }

  @Test
  public void testSubmitCallableIsLazyAndRunsOnCallerThread() throws Exception {
    AtomicBoolean executed = new AtomicBoolean(false);
    AtomicReference<Thread> executionThread = new AtomicReference<>();

    Callable<String> task =
        () -> {
          executed.set(true);
          executionThread.set(Thread.currentThread());
          return "success";
        };

    Future<String> future = executorService.submit(task);

    // Verify it has not executed yet
    assertThat(executed.get()).isFalse();

    // Call get() to trigger execution
    String result = future.get();

    // Verify execution happened and result is correct
    assertThat(executed.get()).isTrue();
    assertThat(result).isEqualTo("success");

    // Verify it ran on the caller's thread
    assertThat(executionThread.get()).isEqualTo(Thread.currentThread());
  }

  @Test
  public void testSubmitRunnableIsLazy() throws Exception {
    AtomicBoolean executed = new AtomicBoolean(false);

    Runnable task = () -> executed.set(true);

    Future<?> future = executorService.submit(task);

    // Verify it has not executed yet
    assertThat(executed.get()).isFalse();

    // Call get() to trigger execution
    future.get();

    // Verify execution happened
    assertThat(executed.get()).isTrue();
  }

  @Test
  public void testShutdownThrowsCancellationExceptionOnGet() {
    AtomicBoolean executed = new AtomicBoolean(false);
    Callable<String> task =
        () -> {
          executed.set(true);
          return "success";
        };

    Future<String> future = executorService.submit(task);
    assertThat(executed.get()).isFalse();

    // Shutdown the executor
    executorService.shutdown();
    assertThat(executorService.isShutdown()).isTrue();

    // Call get() and expect CancellationException
    assertThrows(CancellationException.class, future::get);

    // Verify task was never executed
    assertThat(executed.get()).isFalse();
  }

  @Test
  public void testShutdownNowReturnsEmptyListAndCancelsFutureTaskExecution() {
    AtomicBoolean executed = new AtomicBoolean(false);
    Callable<String> task =
        () -> {
          executed.set(true);
          return "success";
        };

    Future<String> future = executorService.submit(task);

    // shutdownNow should return an empty list since we don't track tasks
    assertThat(executorService.shutdownNow()).isEmpty();
    assertThat(executorService.isShutdown()).isTrue();

    assertThrows(CancellationException.class, future::get);
    assertThat(executed.get()).isFalse();
  }

  @Test
  public void testSubmitCallableIsLazyWithTimeout() throws Exception {
    AtomicBoolean executed = new AtomicBoolean(false);
    Callable<String> task =
        () -> {
          executed.set(true);
          return "success";
        };
    Future<String> future = executorService.submit(task);
    assertThat(executed.get()).isFalse();

    String result = future.get(10, java.util.concurrent.TimeUnit.SECONDS);
    assertThat(executed.get()).isTrue();
    assertThat(result).isEqualTo("success");
  }

  @Test
  public void testSubmitRunnableIsLazyWithTimeout() throws Exception {
    AtomicBoolean executed = new AtomicBoolean(false);
    Runnable task = () -> executed.set(true);
    Future<?> future = executorService.submit(task);
    assertThat(executed.get()).isFalse();

    future.get(10, java.util.concurrent.TimeUnit.SECONDS);
    assertThat(executed.get()).isTrue();
  }

  @Test
  public void testShutdownThrowsCancellationExceptionOnGetWithTimeout() {
    AtomicBoolean executed = new AtomicBoolean(false);
    Callable<String> task =
        () -> {
          executed.set(true);
          return "success";
        };
    Future<String> future = executorService.submit(task);
    executorService.shutdown();

    assertThrows(
        CancellationException.class, () -> future.get(10, java.util.concurrent.TimeUnit.SECONDS));
    assertThat(executed.get()).isFalse();
  }

  @Test
  public void testAwaitTerminationAndIsTerminated() throws Exception {
    assertThat(executorService.isTerminated()).isFalse();
    executorService.shutdown();
    assertThat(executorService.isTerminated()).isTrue();
    assertThat(executorService.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS))
        .isTrue();
  }
}
