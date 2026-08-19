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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A lightweight, lazy ExecutorService that defers task execution until {@code Future.get()} is
 * called. Execution happens synchronously on the thread that invokes {@code get()}.
 *
 * <p>A returned Future represents a pending task. Upon the first invocation of its {@code get()}
 * method, the task executes and its result is permanently cached.
 *
 * <p>Timeout Limitation: Because execution is strictly synchronous on the caller's thread, true
 * preemptive timeouts are not supported. If a timeout is provided to {@code get()}, it only checks
 * if the timeout has already elapsed prior to execution. If execution begins, it will block
 * indefinitely until the task completes, ignoring the timeout duration during execution.
 *
 * <p>Both this class and the returned Future are thread-safe.
 */
final class LazyExecutorService extends AbstractExecutorService {

  private volatile boolean isShutdown = false;
  private volatile boolean isShutdownNow = false;

  @Override
  public void shutdown() {
    isShutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    isShutdown = true;
    isShutdownNow = true;
    return Collections.emptyList();
  }

  @Override
  public boolean isShutdown() {
    return isShutdown;
  }

  /** Returns true if the executor has been shut down, since there are no asynchronous tasks. */
  @Override
  public boolean isTerminated() {
    return isShutdown;
  }

  /** Returns whether the executor has been shut down. */
  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) {
    return isShutdown;
  }

  @Override
  public void execute(Runnable command) {
    throw new RejectedExecutionException("Use submit instead of execute.");
  }

  /**
   * Bulk execution operations (invokeAll, invokeAny) are not supported by this lazy executor. Tasks
   * must be explicitly submitted and resolved individually via their returned Futures.
   */
  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
    throw new UnsupportedOperationException("LazyExecutorService does not support invokeAll");
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException("LazyExecutorService does not support invokeAll");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
    throw new UnsupportedOperationException("LazyExecutorService does not support invokeAny");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException("LazyExecutorService does not support invokeAny");
  }

  @Override
  public Future<?> submit(Runnable task) {
    return submit(Executors.callable(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return submit(Executors.callable(task, result));
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    checkNotNull(task, "task should not be null");
    if (isShutdown) {
      throw new RejectedExecutionException("Executor is shut down");
    }
    return newTaskFor(task);
  }

  private final class LazyFutureTask<V> extends FutureTask<V> {
    LazyFutureTask(Callable<V> callable) {
      super(callable);
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      if (!isDone()) {
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
        if (isShutdownNow) {
          cancel(false);
        } else {
          run(); // Execute on the caller's thread when get() is called
        }
      }
      return super.get();
    }

    /**
     * Note: Because this implementation executes the task synchronously on the calling thread, the
     * provided timeout is inherently ignored during the actual execution of the task. The calling
     * thread will remain blocked until {@code run()} completes, at which point the timeout logic
     * evaluates. True preemptive timeouts are not supported.
     */
    @Override
    public V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (!isDone()) {
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
        if (timeout <= 0) {
          throw new TimeoutException();
        }
        if (isShutdownNow) {
          cancel(false);
        } else {
          run();
        }
      }
      return super.get(timeout, unit);
    }
  }

  /** Wraps the given callable into a LazyFutureTask that overrides get() to execute the task. */
  @Override
  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    return new LazyFutureTask<>(callable);
  }
}
