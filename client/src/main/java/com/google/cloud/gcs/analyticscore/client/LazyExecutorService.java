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
 * A lightweight, lazy ExecutorService that defers task execution until Future.get() is called.
 * Execution happens synchronously on the thread that calls get().
 */
public class LazyExecutorService extends AbstractExecutorService {

  private volatile boolean isShutdown = false;

  @Override
  public void shutdown() {
    isShutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    isShutdown = true;
    return Collections.emptyList();
  }

  @Override
  public boolean isShutdown() {
    return isShutdown;
  }

  @Override
  public boolean isTerminated() {
    return isShutdown;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) {
    return true;
  }

  @Override
  public void execute(Runnable command) {
    throw new RejectedExecutionException("Use submit instead of execute.");
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
    if (task == null) throw new NullPointerException();
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
      if (!isDone() && !isCancelled()) {
        if (isShutdown) {
          cancel(false);
        } else {
          run(); // Execute on the caller's thread when get() is called
        }
      }
      return super.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (!isDone() && !isCancelled()) {
        if (isShutdown) {
          cancel(false);
        } else {
          run();
        }
      }
      return super.get(timeout, unit);
    }
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    return new LazyFutureTask<>(callable);
  }
}
