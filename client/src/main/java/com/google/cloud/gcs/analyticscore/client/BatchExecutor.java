/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs independent GCS requests, such as object deletes or moves, on a bounded thread pool.
 *
 * <p>Create one instance per logical operation and call {@link #shutdown()} when it is done. An
 * instance cannot be reused after shutdown.
 *
 * <p><b>Threading contract:</b> a single owner thread calls {@link #queue}, {@link
 * #awaitRequestsCompletion()} and {@link #shutdown()}. Callbacks run on pool threads, or on the
 * owner thread when the pool is saturated or {@code numThreads} is 0, and may call {@link #queue}
 * before returning (for example to retry a request). Calling {@code awaitRequestsCompletion()} or
 * {@code shutdown()} concurrently from several threads is not supported.
 *
 * <p><b>Failures:</b> pass a callback that records failures to handle per-request errors; the
 * request's future then completes normally. Exceptions from requests without a callback, {@link
 * Error}s, and exceptions thrown by callbacks are reported by {@link #awaitRequestsCompletion()}
 * and {@link #shutdown()}, which wait for every queued request before throwing. If the calling
 * thread is interrupted, the remaining requests are cancelled instead and may still be running
 * after the method throws (see {@link #awaitRequestsCompletion()}).
 */
final class BatchExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(BatchExecutor.class);

  // TODO: Benchmark and determine the optimal default batch executor configuration; currently
  // mirrors hadoop-connectors defaults.
  /** Keep-alive time for idle pool threads. */
  private static final long KEEP_ALIVE_SECONDS = 10L;

  /** Multiplier bounding the task queue size relative to thread count. */
  private static final int QUEUE_CAPACITY_PER_THREAD = 20;

  /** Maximum number of later failures attached as suppressed to the first failure. */
  static final int MAX_SUPPRESSED = 100;

  /** Grace period for pool termination; only after an interrupt may requests still be running. */
  private static final long SHUTDOWN_GRACE_PERIOD_SECONDS = 1L;

  private final ExecutorService requestsExecutor;
  private final Queue<Future<Void>> responseFutures = new ConcurrentLinkedQueue<>();

  /** Number of {@link #queue} calls between {@link #dropUnreportableFutures()} scans. */
  private final int harvestInterval;

  private final AtomicInteger queueCalls = new AtomicInteger();

  BatchExecutor(int numThreads) {
    checkArgument(
        numThreads >= 0 && numThreads <= GcsClientOptions.MAX_BATCH_THREADS,
        "numThreads must be between 0 and %s, got %s",
        GcsClientOptions.MAX_BATCH_THREADS,
        numThreads);
    this.requestsExecutor =
        numThreads == 0 ? newDirectExecutorService() : newRequestExecutor(numThreads);
    this.harvestInterval = Math.max(1, numThreads) * (QUEUE_CAPACITY_PER_THREAD + 2);
  }

  private static ThreadPoolExecutor newRequestExecutor(int numThreads) {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            /* corePoolSize= */ numThreads,
            /* maximumPoolSize= */ numThreads,
            KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(numThreads * QUEUE_CAPACITY_PER_THREAD),
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-batch-executor-pool-%d")
                .setDaemon(true)
                .build());
    executor.allowCoreThreadTimeOut(true);
    // When saturated, run the request on the submitting thread (backpressure). Once shut down,
    // reject it instead of silently dropping it as CallerRunsPolicy would.
    executor.setRejectedExecutionHandler(
        (request, pool) -> {
          if (pool.isShutdown()) {
            throw new RejectedExecutionException("BatchExecutor has been shut down");
          }
          request.run();
        });
    return executor;
  }

  /**
   * Queues {@code task}. If {@code callback} is non-null, it receives the task's result or failure.
   *
   * @throws IllegalStateException if this executor has been shut down
   */
  public <T> void queue(Callable<T> task, @Nullable FutureCallback<? super T> callback) {
    checkNotNull(task, "task must not be null");
    checkState(!requestsExecutor.isShutdown(), "BatchExecutor has been shut down");
    try {
      responseFutures.add(
          requestsExecutor.submit(
              () -> {
                execute(task, callback);
                return null;
              }));
    } catch (RejectedExecutionException e) {
      throw new IllegalStateException("BatchExecutor has been shut down", e);
    }
    if (queueCalls.incrementAndGet() % harvestInterval == 0) {
      dropUnreportableFutures();
    }
  }

  /**
   * Drops completed futures whose outcome {@link #awaitRequestsCompletion()} would never report, so
   * memory and scan cost stay bounded: futures that completed normally, and failed or cancelled
   * futures beyond the first {@code MAX_SUPPRESSED + 1} in queue order.
   */
  private void dropUnreportableFutures() {
    int failed = 0;
    for (Iterator<Future<Void>> it = responseFutures.iterator(); it.hasNext(); ) {
      Future<Void> future = it.next();
      if (future.isDone() && (completedNormally(future) || ++failed > MAX_SUPPRESSED + 1)) {
        it.remove();
      }
    }
  }

  private static boolean completedNormally(Future<Void> doneFuture) {
    try {
      Uninterruptibles.getUninterruptibly(doneFuture);
      return true;
    } catch (ExecutionException | CancellationException e) {
      return false;
    }
  }

  private static <T> void execute(Callable<T> task, @Nullable FutureCallback<? super T> callback)
      throws Exception {
    T result;
    try {
      result = task.call();
    } catch (Throwable t) {
      if (t instanceof InterruptedException) {
        // Preserve the interrupt status of the thread that ran the task.
        Thread.currentThread().interrupt();
      }
      if (callback == null) {
        throw t;
      }
      try {
        callback.onFailure(t);
      } catch (Throwable callbackFailure) {
        if (callbackFailure != t) {
          callbackFailure.addSuppressed(t);
        }
        throw callbackFailure;
      }
      if (t instanceof Error) {
        throw (Error) t; // Never swallow Errors such as OutOfMemoryError.
      }
      return;
    }
    // Outside the try block so that an exception from onSuccess never reaches onFailure.
    if (callback != null) {
      callback.onSuccess(result);
    }
  }

  /**
   * Returns whether the pool has no running or queued requests, for detecting stalls. Always {@code
   * true} when {@code numThreads} is 0.
   */
  public boolean isIdle() {
    if (requestsExecutor instanceof ThreadPoolExecutor) {
      ThreadPoolExecutor executor = (ThreadPoolExecutor) requestsExecutor;
      return executor.getActiveCount() == 0 && executor.getQueue().isEmpty();
    }
    return true;
  }

  @VisibleForTesting
  int getPendingFuturesCount() {
    return responseFutures.size();
  }

  /**
   * Waits for every queued request, including requests that callbacks queue while it waits, and
   * then throws the first failure with up to {@link #MAX_SUPPRESSED} later failures suppressed. The
   * executor stays usable, so multi-phase operations can call this between phases.
   *
   * @throws InterruptedIOException if the calling thread is interrupted while waiting for an
   *     unfinished request (remaining requests are cancelled and the interrupt status is restored),
   *     or if the first failed request was itself interrupted or threw an InterruptedIOException
   *     (all requests were awaited; an interrupted request that ran on the calling thread leaves
   *     the interrupt status set)
   */
  void awaitRequestsCompletion() throws IOException {
    IOException failure = null;
    int suppressedCount = 0;
    Future<Void> future;
    while ((future = responseFutures.poll()) != null) {
      try {
        getFromFuture(future);
      } catch (InterruptedException e) {
        future.cancel(true);
        cancelRemainingFutures();
        Thread.currentThread().interrupt();
        InterruptedIOException interrupted =
            new InterruptedIOException("Interrupted while awaiting batch requests");
        interrupted.initCause(e);
        if (failure != null) {
          interrupted.addSuppressed(failure);
        }
        throw interrupted;
      } catch (IOException e) {
        if (failure == null) {
          failure = e;
        } else if (e != failure && suppressedCount < MAX_SUPPRESSED) {
          failure.addSuppressed(e);
          suppressedCount++;
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  /** Waits for {@code future} and translates its failure into an {@link IOException}. */
  private static void getFromFuture(Future<Void> future) throws IOException, InterruptedException {
    try {
      if (future.isDone()) {
        // Done Guava futures (numThreads == 0) throw if interrupted; read the outcome regardless.
        Uninterruptibles.getUninterruptibly(future);
      } else {
        future.get();
      }
    } catch (CancellationException e) {
      throw new IOException("Batch request was cancelled", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof InterruptedException) {
        InterruptedIOException interrupted =
            new InterruptedIOException("Batch request was interrupted");
        interrupted.initCause(cause);
        throw interrupted;
      }
      throw new IOException("Batch request failed", cause);
    }
  }

  private void cancelRemainingFutures() {
    Future<Void> future;
    while ((future = responseFutures.poll()) != null) {
      future.cancel(true);
    }
  }

  /**
   * Waits for all queued requests like {@link #awaitRequestsCompletion()} and then shuts down the
   * pool, even if waiting fails. Calling it again has no effect.
   */
  public void shutdown() throws IOException {
    try {
      awaitRequestsCompletion();
    } finally {
      requestsExecutor.shutdown();
      // Clear the interrupt status so the grace-period wait is not cut short; restored below.
      boolean interrupted = Thread.interrupted();
      try {
        if (!requestsExecutor.awaitTermination(SHUTDOWN_GRACE_PERIOD_SECONDS, TimeUnit.SECONDS)) {
          LOG.warn("Forcibly shutting down batch executor thread pool.");
          requestsExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        interrupted = true;
        requestsExecutor.shutdownNow();
      } finally {
        // Complete futures whose requests never ran so that later waits cannot block forever.
        cancelRemainingFutures();
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
