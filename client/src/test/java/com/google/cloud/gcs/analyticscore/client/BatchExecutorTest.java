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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BatchExecutorTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  /** Mirrors BatchExecutor: the pool queue holds 20 requests per thread. */
  private static final int QUEUE_CAPACITY_PER_THREAD = 20;

  /** Mirrors BatchExecutor: succeeded futures are dropped every max(1, numThreads) * 22 calls. */
  private static final int HARVEST_INTERVAL_PER_THREAD = QUEUE_CAPACITY_PER_THREAD + 2;

  /** Requests block on this gate; tearDown opens it so that a failed test never strands threads. */
  private final CountDownLatch gate = new CountDownLatch(1);

  /** Counted down by every request that blocks on the gate. */
  private final CountDownLatch started = new CountDownLatch(1);

  private final List<BatchExecutor> executors = new ArrayList<>();

  @AfterEach
  void tearDown() {
    Thread.interrupted(); // Never leak an interrupt into later tests.
    gate.countDown();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          for (BatchExecutor executor : executors) {
            try {
              executor.shutdown();
            } catch (IOException ignored) {
              // Expected failures were asserted by the test; unexpected ones already failed it.
            }
          }
        });
  }

  @Test
  void queue_runsTaskOnPoolThreadAndCallsOnSuccess() {
    BatchExecutor executor = newExecutor(2);
    RecordingCallback<Thread> callback = new RecordingCallback<>();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(Thread::currentThread, callback);
          executor.awaitRequestsCompletion();

          assertThat(callback.failures).isEmpty();
          assertThat(callback.successes).containsExactly(callback.thread);
          assertThat(callback.thread).isNotSameInstanceAs(Thread.currentThread());
          assertThat(callback.thread.getName()).startsWith("gcs-batch-executor-pool-");
          assertThat(callback.thread.isDaemon()).isTrue();
        });
  }

  @Test
  void queue_withZeroThreads_runsTaskSynchronouslyOnCallerThread() throws IOException {
    BatchExecutor executor = newExecutor(0); // Never blocks: requests run inside queue().
    RecordingCallback<Thread> callback = new RecordingCallback<>();
    IOException failure = new IOException("request failed");

    executor.queue(Thread::currentThread, callback);

    assertThat(callback.successes).containsExactly(Thread.currentThread());
    assertThat(callback.thread).isSameInstanceAs(Thread.currentThread());
    // Every request has completed, so an owner interrupt neither cancels nor loses any of them.
    executor.queue(failWith(failure), null);
    Thread.currentThread().interrupt();
    IOException thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);
    assertThat(thrown).isSameInstanceAs(failure);
    executor.queue(() -> "ok", null);
    executor.shutdown(); // Does not throw although the owner is still interrupted.
    assertThat(Thread.interrupted()).isTrue();
  }

  @Test
  void queue_whenPoolIsSaturated_runsTaskOnCallerThread() {
    BatchExecutor executor = newExecutor(1);
    AtomicInteger queuedRuns = new AtomicInteger();
    RecordingCallback<Thread> callback = new RecordingCallback<>();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(this::blockOnGate, null);
          started.await();
          for (int i = 0; i < QUEUE_CAPACITY_PER_THREAD; i++) {
            executor.queue(queuedRuns::incrementAndGet, null);
          }
          executor.queue(Thread::currentThread, callback);

          assertThat(callback.successes).containsExactly(Thread.currentThread());
          assertThat(callback.thread).isSameInstanceAs(Thread.currentThread());
          assertThat(queuedRuns.get()).isEqualTo(0);
          gate.countDown();
          executor.awaitRequestsCompletion();
          assertThat(queuedRuns.get()).isEqualTo(QUEUE_CAPACITY_PER_THREAD);
        });
  }

  @Test
  void constructor_withOutOfRangeThreads_throwsIllegalArgumentException() {
    int tooMany = GcsClientOptions.MAX_BATCH_THREADS + 1;

    IllegalArgumentException negative =
        assertThrows(IllegalArgumentException.class, () -> new BatchExecutor(-1));
    IllegalArgumentException excessive =
        assertThrows(IllegalArgumentException.class, () -> new BatchExecutor(tooMany));

    assertThat(negative).hasMessageThat().contains("got -1");
    assertThat(excessive).hasMessageThat().contains("got " + tooMany);
    newExecutor(0); // Both bounds are accepted.
    newExecutor(GcsClientOptions.MAX_BATCH_THREADS);
  }

  @Test
  void queue_whenOnSuccessThrows_neverCallsOnFailureAndAwaitThrows() {
    BatchExecutor executor = newExecutor(2);
    IllegalStateException callbackFailure = new IllegalStateException("onSuccess failed");
    RecordingCallback<String> callback = new RecordingCallback<String>().throwing(callbackFailure);
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(() -> "result", callback);

          IOException thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);

          assertThat(thrown).hasCauseThat().isSameInstanceAs(callbackFailure);
          assertThat(callback.successes).containsExactly("result");
          assertThat(callback.failures).isEmpty();
        });
  }

  @Test
  void queue_whenTaskFailsWithCallback_callsOnFailureAndAwaitDoesNotThrow() {
    BatchExecutor executor = newExecutor(2);
    IOException failure = new IOException("request failed");
    RecordingCallback<Object> callback = new RecordingCallback<>();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(failWith(failure), callback);
          executor.awaitRequestsCompletion();
          assertThat(callback.failures).containsExactly(failure);
          assertThat(callback.successes).isEmpty();
        });
  }

  @Test
  void awaitRequestsCompletion_withoutCallback_rethrowsIOExceptionAndWrapsOthers() {
    BatchExecutor executor = newExecutor(2);
    IOException ioFailure = new IOException("request failed");
    IllegalArgumentException runtimeFailure = new IllegalArgumentException("bad request");
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(failWith(ioFailure), null);
          IOException thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);
          assertThat(thrown).isSameInstanceAs(ioFailure);

          executor.queue(failWith(runtimeFailure), null);
          thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);
          assertThat(thrown).hasCauseThat().isSameInstanceAs(runtimeFailure);
        });
  }

  @Test
  void queue_whenTaskThrowsErrorWithCallback_callsOnFailureAndAwaitRethrowsError() {
    BatchExecutor executor = newExecutor(2);
    IOException earlierFailure = new IOException("earlier request failed");
    OutOfMemoryError error = new OutOfMemoryError("simulated");
    RecordingCallback<Object> callback = new RecordingCallback<>();
    OutOfMemoryError errorWithFailingCallback = new OutOfMemoryError("simulated with callback bug");
    IllegalStateException callbackFailure = new IllegalStateException("onFailure failed");
    RecordingCallback<Object> failingCallback =
        new RecordingCallback<Object>().throwing(callbackFailure);
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(failWith(earlierFailure), null);
          executor.queue(
              () -> {
                throw error;
              },
              callback);

          OutOfMemoryError thrown =
              assertThrows(OutOfMemoryError.class, executor::awaitRequestsCompletion);

          assertThat(callback.failures).containsExactly(error);
          assertThat(thrown).isSameInstanceAs(error);
          assertThat(error.getSuppressed()).asList().containsExactly(earlierFailure);

          executor.queue(
              () -> {
                throw errorWithFailingCallback;
              },
              failingCallback);

          thrown = assertThrows(OutOfMemoryError.class, executor::awaitRequestsCompletion);

          assertThat(failingCallback.failures).containsExactly(errorWithFailingCallback);
          assertThat(thrown).isSameInstanceAs(errorWithFailingCallback);
          assertThat(errorWithFailingCallback.getSuppressed())
              .asList()
              .containsExactly(callbackFailure);
        });
  }

  @Test
  void queue_whenOnFailureThrows_awaitThrowsWithTaskFailureSuppressed() {
    BatchExecutor executor = newExecutor(2);
    IOException taskFailure = new IOException("request failed");
    IllegalStateException callbackFailure = new IllegalStateException("onFailure failed");
    RecordingCallback<Object> callback = new RecordingCallback<Object>().throwing(callbackFailure);
    IllegalArgumentException rethrownException =
        new IllegalArgumentException("rethrown by callback");
    AssertionError rethrownError = new AssertionError("rethrown error by callback");
    FutureCallback<Object> rethrowingCallback =
        new RecordingCallback<Object>() {
          @Override
          public void onFailure(Throwable t) {
            super.onFailure(t);
            if (t instanceof Error) {
              throw (Error) t;
            }
            throw (RuntimeException) t;
          }
        };
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(failWith(taskFailure), callback);

          IOException thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);

          assertThat(callback.failures).containsExactly(taskFailure);
          assertThat(thrown).hasCauseThat().isSameInstanceAs(callbackFailure);
          assertThat(callbackFailure.getSuppressed()).asList().containsExactly(taskFailure);

          executor.queue(failWith(rethrownException), rethrowingCallback);
          thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);
          assertThat(thrown).hasCauseThat().isSameInstanceAs(rethrownException);
          assertThat(rethrownException.getSuppressed()).isEmpty();

          executor.queue(
              () -> {
                throw rethrownError;
              },
              rethrowingCallback);
          AssertionError thrownError =
              assertThrows(AssertionError.class, executor::awaitRequestsCompletion);
          assertThat(thrownError).isSameInstanceAs(rethrownError);
          assertThat(rethrownError.getSuppressed()).isEmpty();
        });
  }

  @Test
  void awaitRequestsCompletion_drainsAllRequestsBeforeThrowingFirstFailure() {
    BatchExecutor executor = newExecutor(4);
    IOException first = new IOException("first");
    IOException second = new IOException("second");
    IOException third = new IOException("third");
    RecordingCallback<Thread> slow = new RecordingCallback<>();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(failWith(first), null);
          for (int i = 0; i < 3; i++) {
            executor.queue(this::blockOnGate, slow);
          }
          executor.queue(failWith(second), null);
          executor.queue(failWith(third), null);
          // Release the slow requests only once the barrier has consumed the first failure.
          runWhen(() -> executor.getPendingFuturesCount() <= 4, gate::countDown);

          IOException thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);

          assertThat(slow.successes).hasSize(3);
          assertThat(slow.successes).containsNoDuplicates(); // One core pool thread per request.
          assertThat(thrown).isSameInstanceAs(first);
          assertThat(thrown.getSuppressed()).asList().containsExactly(second, third).inOrder();
          assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
        });
  }

  @Test
  void awaitRequestsCompletion_capsSuppressedFailures() {
    BatchExecutor executor = newExecutor(0); // Never blocks, and harvest counts are exact.
    int reportable = BatchExecutor.MAX_SUPPRESSED + 1;
    List<IOException> failures = new ArrayList<>();
    for (int i = 0; i < reportable + 2 * HARVEST_INTERVAL_PER_THREAD; i++) {
      IOException failure = new IOException("failure " + i);
      failures.add(failure);
      executor.queue(failWith(failure), null);
    }
    // Harvests also drop the failures that awaitRequestsCompletion() could never report.
    assertThat(executor.getPendingFuturesCount())
        .isLessThan(reportable + HARVEST_INTERVAL_PER_THREAD);

    IOException thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);

    List<IOException> later = failures.subList(1, reportable);
    assertThat(thrown).isSameInstanceAs(failures.get(0));
    assertThat(thrown.getSuppressed()).asList().containsExactlyElementsIn(later).inOrder();
    assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
  }

  @Test
  void awaitRequestsCompletion_whenErrorFollowsMaxSuppressedIoExceptions_retainsAndThrowsError() {
    BatchExecutor executor = newExecutor(0); // Never blocks, and harvest counts are exact.
    int reportable = BatchExecutor.MAX_SUPPRESSED + 1;
    List<IOException> ioFailures = new ArrayList<>();
    for (int i = 0; i < reportable + HARVEST_INTERVAL_PER_THREAD; i++) {
      IOException failure = new IOException("failure " + i);
      ioFailures.add(failure);
      executor.queue(failWith(failure), null);
    }
    AssertionError error = new AssertionError("late fatal error");
    executor.queue(
        () -> {
          throw error;
        },
        null);
    for (int i = 0; i < HARVEST_INTERVAL_PER_THREAD; i++) {
      executor.queue(() -> null, null);
    }

    AssertionError thrown = assertThrows(AssertionError.class, executor::awaitRequestsCompletion);

    assertThat(thrown).isSameInstanceAs(error);
    assertThat(thrown.getSuppressed()).asList().containsExactly(ioFailures.get(0));
    assertThat(ioFailures.get(0).getSuppressed())
        .asList()
        .containsExactlyElementsIn(ioFailures.subList(1, reportable))
        .inOrder();
  }

  @Test
  void awaitRequestsCompletion_whenRequestsShareOneException_throwsItWithoutSelfSuppression() {
    BatchExecutor executor = newExecutor(0); // Never blocks: requests run inside queue().
    IOException shared = new IOException("shared failure");
    executor.queue(failWith(shared), null);
    executor.queue(failWith(shared), null);

    // Suppressing the failure into itself would throw IllegalArgumentException instead.
    IOException thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);

    assertThat(thrown).isSameInstanceAs(shared);
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  @Test
  void awaitRequestsCompletion_isReusablePhaseBarrier() {
    BatchExecutor executor = newExecutor(4);
    int phaseSize = 8;
    IOException phase1Failure = new IOException("phase 1 failed");
    RecordingCallback<Thread> phase1 = new RecordingCallback<>();
    RecordingCallback<Integer> phase2 = new RecordingCallback<>();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(failWith(phase1Failure), null);
          for (int i = 0; i < phaseSize; i++) {
            executor.queue(this::blockOnGate, phase1);
          }
          // Release phase 1 only once the barrier waits for one of its blocked requests.
          runWhen(() -> executor.getPendingFuturesCount() < phaseSize, gate::countDown);
          IOException thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);
          assertThat(thrown).isSameInstanceAs(phase1Failure);

          for (int i = 0; i < phaseSize; i++) {
            executor.queue(phase1.successes::size, phase2); // Phase 1 requests finished so far.
          }
          executor.awaitRequestsCompletion(); // Does not report the phase 1 failure again.
          assertThat(phase2.successes)
              .containsExactlyElementsIn(Collections.nCopies(phaseSize, phaseSize));
        });
  }

  @Test
  void awaitRequestsCompletion_waitsForRetryQueuedByCallback() {
    BatchExecutor executor = newExecutor(1);
    RecordingCallback<Thread> retry = new RecordingCallback<>();
    FutureCallback<Object> retryOnFailure =
        new RecordingCallback<Object>() {
          @Override
          public void onFailure(Throwable t) {
            executor.queue(Thread::currentThread, retry);
          }
        };
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(this::blockOnGate, null); // Keeps the only pool thread busy.
          executor.queue(failWith(new IOException("transient")), retryOnFailure);
          // The failing request runs, and queues its retry, only once the barrier is waiting.
          runWhen(() -> executor.getPendingFuturesCount() <= 1, gate::countDown);

          executor.awaitRequestsCompletion();

          assertThat(retry.successes).hasSize(1);
          assertThat(executor.getPendingFuturesCount()).isEqualTo(0); // The retry was awaited.
        });
  }

  @Test
  void queue_withManyFastTasks_keepsPendingFuturesBounded() {
    int numThreads = 4;
    BatchExecutor executor = newExecutor(numThreads);
    AtomicInteger completed = new AtomicInteger();
    // Two harvest intervals of new futures plus every running or queued request.
    int bound = numThreads * HARVEST_INTERVAL_PER_THREAD * 2 + numThreads * 21 + 1;
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          int maxPending = 0;
          for (int i = 0; i < 10_000; i++) {
            executor.queue(completed::incrementAndGet, null);
            maxPending = Math.max(maxPending, executor.getPendingFuturesCount());
          }
          executor.awaitRequestsCompletion();

          assertThat(maxPending).isAtMost(bound);
          assertThat(completed.get()).isEqualTo(10_000);
          assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
        });
  }

  @Test
  void queue_whenHarvestingSucceededFutures_keepsFailedOnesForAwait() {
    BatchExecutor executor = newExecutor(0); // Never blocks, and counts are exact.
    IOException first = new IOException("first");
    IOException second = new IOException("second");

    executor.queue(failWith(first), null);
    for (int call = 2; call <= 3 * HARVEST_INTERVAL_PER_THREAD; call++) { // Harvests 3 times.
      executor.queue(call == HARVEST_INTERVAL_PER_THREAD + 1 ? failWith(second) : () -> "ok", null);
    }

    assertThat(executor.getPendingFuturesCount()).isEqualTo(2);
    IOException thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);
    assertThat(thrown).isSameInstanceAs(first);
    assertThat(thrown.getSuppressed()).asList().containsExactly(second);
  }

  @Test
  void awaitRequestsCompletion_whenOwnerInterrupted_throwsAndCancelsRemainingRequests() {
    BatchExecutor executor = newExecutor(1);
    RecordingCallback<Thread> blocked =
        new RecordingCallback<Thread>() {
          @Override
          public void onFailure(Throwable t) {
            super.onFailure(t);
            Uninterruptibles.awaitUninterruptibly(gate); // Holds the only pool thread.
          }
        };
    RecordingCallback<Thread> queued = new RecordingCallback<>();
    IOException earlier = new IOException("earlier");
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(failWith(earlier), null); // Must neither be lost nor mask the interrupt.
          executor.queue(this::blockOnGate, blocked);
          executor.queue(Thread::currentThread, queued); // Waits in the pool queue.
          started.await();
          Thread owner = Thread.currentThread();
          // Interrupt the owner once the barrier has taken the blocked request and waits for it.
          runWhen(() -> executor.getPendingFuturesCount() <= 1, owner::interrupt);

          InterruptedIOException thrown =
              assertThrows(InterruptedIOException.class, executor::awaitRequestsCompletion);

          assertThat(Thread.interrupted()).isTrue(); // Restored; cleared for the waits below.
          assertThat(thrown).hasCauseThat().isInstanceOf(InterruptedException.class);
          assertThat(thrown.getSuppressed()).asList().containsExactly(earlier);
          blocked.called.await(); // The gate is closed, so only cancel(true) can end the request.
          assertThat(blocked.failures.get(0)).isInstanceOf(InterruptedException.class);
          gate.countDown(); // The pool thread now runs whatever is left in its queue.
          assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
          executor.shutdown();
          assertThat(queued.called.getCount()).isEqualTo(1); // Cancelled before it could run.
        });
  }

  @Test
  void awaitRequestsCompletion_whenPoolTaskThrowsInterruptedException_doesNotInterruptOwner() {
    BatchExecutor executor = newExecutor(1);
    InterruptedException interrupt = new InterruptedException("request interrupted");
    RecordingCallback<Object> callback = new RecordingCallback<>();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(failWith(interrupt), null);
          executor.queue(failWith(new InterruptedException()), callback);

          InterruptedIOException thrown =
              assertThrows(InterruptedIOException.class, executor::awaitRequestsCompletion);

          assertThat(thrown).hasCauseThat().isSameInstanceAs(interrupt);
          assertThat(Thread.currentThread().isInterrupted()).isFalse();
          assertThat(callback.interrupted).isTrue(); // Restored on the pool thread.
        });
  }

  @Test
  void shutdown_waitsForInFlightRequestsThenTerminatesPool() {
    BatchExecutor executor = newExecutor(2);
    RecordingCallback<Thread> callback = new RecordingCallback<>();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(this::blockOnGate, callback);
          started.await();
          // Release the request only once shutdown() has taken its future and waits for it.
          runWhen(() -> executor.getPendingFuturesCount() == 0, gate::countDown);

          executor.shutdown();

          assertThat(callback.successes).hasSize(1);
          assertTerminates(callback.thread);
          assertThrows(IllegalStateException.class, () -> executor.queue(() -> null, null));
        });
  }

  @Test
  void shutdown_isIdempotentAndRejectsLaterRequests() {
    AtomicInteger runs = new AtomicInteger();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          for (int numThreads : new int[] {0, 2}) {
            BatchExecutor executor = newExecutor(numThreads);
            executor.queue(runs::incrementAndGet, null);
            executor.shutdown();
            executor.shutdown();
            IllegalStateException e =
                assertThrows(
                    IllegalStateException.class, () -> executor.queue(runs::incrementAndGet, null));
            assertThat(e).hasMessageThat().contains("shut down");
          }
          assertThat(runs.get()).isEqualTo(2); // Rejected requests never ran.
        });
  }

  @Test
  void shutdown_whenAwaitThrows_stillShutsDownPool() {
    BatchExecutor executor = newExecutor(1);
    IOException failure = new IOException("request failed");
    RecordingCallback<Thread> callback = new RecordingCallback<>();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          executor.queue(Thread::currentThread, callback); // Records the only pool thread.
          executor.queue(failWith(failure), null);

          IOException thrown = assertThrows(IOException.class, executor::shutdown);

          assertThat(thrown).isSameInstanceAs(failure);
          assertThrows(IllegalStateException.class, () -> executor.queue(() -> null, null));
          assertTerminates(callback.thread);
          executor.shutdown(); // The failure was reported once; calling again has no effect.
        });
  }

  @Test
  void close_inTryWithResources_shutsDownPoolAndSuppressesTaskFailureOnPrimaryException() {
    IOException primaryFailure = new IOException("listing failed with 403");
    IOException taskFailure = new IOException("queued delete failed");
    RecordingCallback<Thread> callback = new RecordingCallback<>();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          BatchExecutor[] closedExecutor = new BatchExecutor[1];
          IOException thrown =
              assertThrows(
                  IOException.class,
                  () -> {
                    try (BatchExecutor executor = newExecutor(1)) {
                      closedExecutor[0] = executor;
                      executor.queue(Thread::currentThread, callback);
                      executor.queue(failWith(taskFailure), null);
                      throw primaryFailure;
                    }
                  });

          assertThat(thrown).isSameInstanceAs(primaryFailure);
          assertThat(thrown.getSuppressed()).asList().containsExactly(taskFailure);
          assertThrows(
              IllegalStateException.class, () -> closedExecutor[0].queue(() -> null, null));
          assertTerminates(callback.thread);
        });
  }

  @Test
  void shutdown_withInterruptedCaller_terminatesPoolAndPreservesInterrupt() {
    BatchExecutor idle = newExecutor(1);
    BatchExecutor busy = newExecutor(1);
    RecordingCallback<Thread> idleCallback = new RecordingCallback<>();
    RecordingCallback<Thread> busyCallback = new RecordingCallback<>();
    RecordingCallback<Thread> queuedCallback = new RecordingCallback<>();
    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          idle.queue(Thread::currentThread, idleCallback);
          idle.awaitRequestsCompletion(); // The pool thread stays alive for its keep-alive time.
          Thread.currentThread().interrupt();
          idle.shutdown();
          assertThat(Thread.interrupted()).isTrue();
          assertTerminates(idleCallback.thread);

          busy.queue(
              () -> {
                try {
                  return blockOnGate();
                } catch (InterruptedException cancelled) { // Survives cancel(true)'s interrupt.
                  return blockOnGate();
                }
              },
              busyCallback);
          started.await();
          // Waits in the pool queue behind the blocked request until shutdownNow() drains it.
          busy.queue(Thread::currentThread, queuedCallback);
          Thread.currentThread().interrupt();
          long start = System.nanoTime(); // shutdown() must wait out its 1 s grace period first.
          assertThrows(InterruptedIOException.class, busy::shutdown);
          assertThat(System.nanoTime() - start).isAtLeast(Duration.ofSeconds(1).toNanos());
          assertThat(Thread.interrupted()).isTrue();
          busyCallback.called.await(); // Only shutdownNow() can interrupt the request again.
          assertThat(busyCallback.failures.get(0)).isInstanceOf(InterruptedException.class);
          assertTerminates(busyCallback.thread);
          assertThat(queuedCallback.called.getCount()).isEqualTo(1); // It never ran.
          assertThrows(IllegalStateException.class, () -> busy.queue(() -> null, null));
        });
  }

  private BatchExecutor newExecutor(int numThreads) {
    BatchExecutor executor = new BatchExecutor(numThreads);
    executors.add(executor);
    return executor;
  }

  /** A request that counts down started, waits for the gate and returns the thread it ran on. */
  private Thread blockOnGate() throws InterruptedException {
    started.countDown();
    gate.await();
    return Thread.currentThread();
  }

  private static Callable<Object> failWith(Exception failure) {
    return () -> {
      throw failure;
    };
  }

  /**
   * Runs {@code action} on a helper thread once {@code condition} holds (or tearDown opens the
   * gate). awaitRequestsCompletion() removes each future before waiting for it, so conditions on
   * getPendingFuturesCount() show how far the owner thread has progressed.
   */
  private void runWhen(BooleanSupplier condition, Runnable action) {
    Thread helper =
        new Thread(
            () -> {
              while (!condition.getAsBoolean() && gate.getCount() > 0) {
                Thread.onSpinWait();
              }
              if (condition.getAsBoolean()) {
                action.run();
              }
            });
    helper.setDaemon(true);
    helper.start();
  }

  /** Asserts that {@code thread} exits well within the 10 s keep-alive of idle pool threads. */
  private static void assertTerminates(Thread thread) throws InterruptedException {
    thread.join(TIMEOUT.toMillis() / 2);
    assertThat(thread.isAlive()).isFalse();
  }

  /** Records each callback call and the state of its thread, then throws {@code toThrow} if set. */
  private static class RecordingCallback<T> implements FutureCallback<T> {
    final List<T> successes = new CopyOnWriteArrayList<>();
    final List<Throwable> failures = new CopyOnWriteArrayList<>();
    final CountDownLatch called = new CountDownLatch(1);
    volatile Thread thread;
    volatile boolean interrupted;
    @Nullable private RuntimeException toThrow;

    RecordingCallback<T> throwing(RuntimeException e) {
      toThrow = e;
      return this;
    }

    @Override
    public void onSuccess(T result) {
      successes.add(result);
      record();
    }

    @Override
    public void onFailure(Throwable t) {
      failures.add(t);
      record();
    }

    private void record() {
      thread = Thread.currentThread();
      interrupted = thread.isInterrupted();
      called.countDown();
      if (toThrow != null) {
        throw toThrow;
      }
    }
  }
}
