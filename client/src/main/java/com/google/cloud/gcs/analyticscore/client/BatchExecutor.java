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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.MoreExecutors;
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
 * <p>Create one instance per logical operation and close it (or call {@link #shutdown()}) when it
 * is done. An instance cannot be reused after shutdown.
 *
 * <p><b>Threading contract:</b> a single owner thread calls {@link #queue}, {@link
 * #awaitRequestsCompletion()} and {@link #shutdown()} (or {@link #close()}). Callbacks run on pool
 * threads, or on the owner thread when the pool is saturated or {@code numThreads} is 0, and may
 * call {@link #queue} before returning (for example to retry a request, though callbacks must not
 * re-queue on {@link InterruptedException} or {@link CancellationException}). Calling {@code
 * awaitRequestsCompletion()} or {@code shutdown()} concurrently from several threads is not
 * supported.
 *
 * <p><b>Failures:</b> pass a callback that records failures to handle per-request errors; the
 * request's future then completes normally ({@code onFailure} for an interrupted request runs with
 * the thread's interrupt status set). Exceptions from requests without a callback, {@link Error}s,
 * and exceptions thrown by callbacks are reported by {@link #awaitRequestsCompletion()} and {@link
 * #shutdown()}, which wait for every queued request before throwing. If the calling thread is
 * interrupted, the remaining requests are cancelled instead and may still be running after the
 * method throws (see {@link #awaitRequestsCompletion()}).
 */
final class BatchExecutor implements AutoCloseable {

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
  private boolean closed;

  BatchExecutor(int numThreads) {
    checkArgument(
        numThreads >= 0 && numThreads <= GcsClientOptions.MAX_BATCH_THREADS,
        "numThreads must be between 0 and %s, got %s",
        GcsClientOptions.MAX_BATCH_THREADS,
        numThreads);
    this.requestsExecutor =
        numThreads == 0 ? MoreExecutors.newDirectExecutorService() : newRequestExecutor(numThreads);
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
  <T> void queue(Callable<T> task, @Nullable FutureCallback<? super T> callback) {
    checkNotNull(task, "task must not be null");
    checkState(!requestsExecutor.isShutdown(), "BatchExecutor has been shut down");
    Future<Void> future;
    try {
      future =
          requestsExecutor.submit(
              () -> {
                execute(task, callback);
                return null;
              });
    } catch (RejectedExecutionException e) {
      throw new IllegalStateException("BatchExecutor has been shut down", e);
    }
    responseFutures.add(future);
    if (requestsExecutor.isShutdown()) {
      future.cancel(false);
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
        if (t instanceof Error) {
          if (callbackFailure != t) {
            t.addSuppressed(callbackFailure);
          }
          throw (Error) t;
        }
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

  @VisibleForTesting
  int getPendingFuturesCount() {
    return responseFutures.size();
  }

  /**
   * Waits for every queued request, including requests that callbacks queue while it waits, and
   * then throws the first failure with up to {@link #MAX_SUPPRESSED} later failures suppressed
   * (prioritizing {@link Error} over {@link IOException}). The executor stays usable, so
   * multi-phase operations can call this between phases.
   *
   * <p>Queue every request of the current phase before calling this method. Only requests queued by
   * the owner thread before the call, or by callbacks of the requests it awaits, are awaited.
   *
   * @throws InterruptedIOException if the calling thread is interrupted while waiting for an
   *     unfinished request (remaining requests are cancelled and the interrupt status is restored),
   *     or if the first failed request was itself interrupted or threw an InterruptedIOException
   *     (all requests were awaited). A request interrupted while running on the calling thread
   *     leaves the calling thread interrupted, so a later call cancels unfinished requests as
   *     described above.
   */
  void awaitRequestsCompletion() throws IOException {
    Throwable failure = null;
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
        if (failure instanceof Error) {
          if (suppressedCount < MAX_SUPPRESSED) {
            failure.addSuppressed(interrupted);
          }
          throw (Error) failure;
        }
        if (failure != null) {
          interrupted.addSuppressed(failure);
        }
        throw interrupted;
      } catch (IOException | Error e) {
        if (failure == null) {
          failure = e;
        } else if (e instanceof Error && !(failure instanceof Error)) {
          if (suppressedCount < MAX_SUPPRESSED) {
            e.addSuppressed(failure);
            suppressedCount++;
          }
          failure = e;
        } else if (e != failure && suppressedCount < MAX_SUPPRESSED) {
          failure.addSuppressed(e);
          suppressedCount++;
        }
      }
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    if (failure != null) {
      throw (IOException) failure;
    }
  }

  /**
   * Waits for {@code future}, rethrowing any {@link Error} directly and translating other failures
   * into an {@link IOException}.
   */
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
      if (cause instanceof Error) {
        throw (Error) cause;
      }
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
   * Stops the pool and cancels the requests it never started, so their futures always complete even
   * if they are tracked after {@link #cancelRemainingFutures()} runs.
   */
  private void forceShutdown() {
    for (Runnable neverStarted : requestsExecutor.shutdownNow()) {
      if (neverStarted instanceof Future) {
        ((Future<?>) neverStarted).cancel(false);
      }
    }
  }

  /** Equivalent to {@link #shutdown()}, for use with try-with-resources. */
  @Override
  public void close() throws IOException {
    shutdown();
  }

  /**
   * Waits for all queued requests like {@link #awaitRequestsCompletion()} and then shuts down the
   * pool, even if waiting fails. Calling it again has no effect.
   */
  void shutdown() throws IOException {
    if (closed) {
      return;
    }
    try {
      awaitRequestsCompletion();
    } finally {
      closed = true;
      requestsExecutor.shutdown();
      // Clear the interrupt status so the grace-period wait is not cut short; restored below.
      boolean interrupted = Thread.interrupted();
      try {
        if (!requestsExecutor.awaitTermination(SHUTDOWN_GRACE_PERIOD_SECONDS, TimeUnit.SECONDS)) {
          LOG.warn("Forcibly shutting down batch executor thread pool.");
          forceShutdown();
        }
      } catch (InterruptedException e) {
        interrupted = true;
        forceShutdown();
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
