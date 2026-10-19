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

import static com.google.common.truth.Truth.assertThat;
import static java.util.Arrays.copyOfRange;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.function.ThrowingConsumer;

/** Runs one read-channel contract against {@link InMemoryStorage} and the SDK's local fake. */
class InMemoryStorageContractTest {
  private static final BlobId BLOB = BlobId.of("bucket", "obj");
  private static final GcsItemId ITEM =
      GcsItemId.builder().setBucketName("bucket").setObjectName("obj").build();
  private static final byte[] CONTENT = TestDataGenerator.generateSeededRandomBytes(64, 7);

  // Each case receives a freshly opened channel. readAll(...) reads until the channel returns -1.
  private static final ImmutableMap<String, ThrowingConsumer<ReadChannel>> CASES =
      ImmutableMap.<String, ThrowingConsumer<ReadChannel>>builder()
          .put("fullRead_returnsContent", ch -> assertThat(readAll(ch)).isEqualTo(CONTENT))
          .put(
              "seek_readsFromOffset",
              ch ->
                  assertThat(readAll(after(ch, c -> c.seek(10))))
                      .isEqualTo(copyOfRange(CONTENT, 10, 64)))
          .put(
              "limit_isAbsoluteEndOffset",
              ch ->
                  assertThat(readAll(after(ch.limit(20), c -> c.seek(10))))
                      .isEqualTo(copyOfRange(CONTENT, 10, 20)))
          // Not run against LocalStorageHelper: its FakeStorageRpc zero-pads the response up to
          // limit when limit > object size, whereas real GCS (and InMemoryStorage) stop at EOF.
          .put(
              "limit_beyondSize_readsToEof",
              ch -> assertThat(readAll(ch.limit(999))).isEqualTo(CONTENT))
          .put(
              "seek_pastEof_readReturnsMinusOne",
              ch ->
                  assertThat(after(ch, c -> c.seek(999)).read(ByteBuffer.allocate(8)))
                      .isEqualTo(-1))
          .put(
              "partialReads_fillBufferAndAdvance",
              ch ->
                  assertThat(Bytes.concat(read(ch, 5), read(ch, 5)))
                      .isEqualTo(copyOfRange(CONTENT, 0, 10)))
          .put(
              "read_afterClose_throwsClosedChannelException",
              ch ->
                  assertThrows(
                      ClosedChannelException.class,
                      () -> after(ch, ReadChannel::close).read(ByteBuffer.allocate(8))))
          .put(
              "setChunkSize_doesNotChangeResult",
              ch -> assertThat(readAll(after(ch, c -> c.setChunkSize(7)))).isEqualTo(CONTENT))
          .buildOrThrow();

  @TestFactory
  Stream<DynamicTest> readChannelContract() {
    Storage local = LocalStorageHelper.getOptions().getService();
    local.create(BlobInfo.newBuilder(BLOB).build(), CONTENT);
    ImmutableMap<String, Storage> impls =
        ImmutableMap.of("InMemoryStorage", inMemory().storage(), "LocalStorageHelper", local);
    return impls.entrySet().stream()
        .flatMap(
            impl ->
                CASES.entrySet().stream()
                    .filter(
                        c -> impl.getValue() != local || !c.getKey().startsWith("limit_beyondSize"))
                    .map(
                        c ->
                            dynamicTest(
                                impl.getKey() + "." + c.getKey(),
                                () -> {
                                  try (ReadChannel ch = impl.getValue().reader(BLOB)) {
                                    c.getValue().accept(ch);
                                  }
                                })));
  }

  @TestFactory
  Stream<DynamicTest> unsupportedMethods_failLoudly() {
    Storage s = inMemory().storage();
    return Stream.of(
        dynamicTest("list", () -> assertUnsupported("list", () -> s.list("bucket"))),
        dynamicTest("get", () -> assertUnsupported("get", () -> s.get(BLOB))),
        dynamicTest(
            "blobReadSession",
            () -> assertUnsupported("blobReadSession", () -> s.blobReadSession(BLOB))));
  }

  @Test
  void objectMethods_areNotFailLoud() {
    Storage s = inMemory().storage();
    Object sameProxy = s;

    assertThat(s.equals(sameProxy)).isTrue();
    assertThat(s.hashCode()).isEqualTo(System.identityHashCode(s));
    assertThat(s.toString()).contains("InMemoryStorage");
  }

  @Test
  void reader_unknownObject_throwsNotFoundOnFirstRead() {
    InMemoryStorage storage = inMemory();

    try (ReadChannel ch = storage.storage().reader(BlobId.of("bucket", "missing"))) {
      assertThat(storage.readerOpens()).isEqualTo(0);
      StorageException e =
          assertThrows(StorageException.class, () -> ch.read(ByteBuffer.allocate(1)));
      assertThat(e.getCode()).isEqualTo(404);
      assertThat(storage.readerOpens()).isEqualTo(0);
    }
  }

  @Test
  void counters_trackLazyOpensSeekAndLimitReopensAndBytes() throws IOException {
    InMemoryStorage storage = inMemory();
    try (ReadChannel ignored = storage.storage().reader(BLOB)) {
      assertThat(storage.readerOpens()).isEqualTo(0);
    }
    try (ReadChannel ch = storage.storage().reader(BLOB)) {
      ch.limit(20);
      ch.seek(0);
      read(ch, 5);
      read(ch, 5);
      ch.seek(10);
      read(ch, 5);
      assertThat(storage.readerOpens()).isEqualTo(1);
      assertThat(storage.bytesServed()).isEqualTo(15);

      ch.seek(0);
      read(ch, 5);
      assertThat(storage.readerOpens()).isEqualTo(2);

      ch.limit(CONTENT.length);
      readAll(ch);
      assertThat(storage.readerOpens()).isEqualTo(3);
      assertThat(storage.bytesServed()).isEqualTo(15 + CONTENT.length);
    }
    storage.resetCounters();
    assertThat(storage.readerOpens() + storage.bytesServed()).isEqualTo(0);
  }

  @Test
  void create_withDecorator_wrapsEveryReader() {
    InMemoryStorage storage =
        InMemoryStorage.create(ImmutableMap.of(ITEM, CONTENT), TrackingReadChannel::new);

    assertThat(storage.storage().reader(BLOB)).isInstanceOf(TrackingReadChannel.class);
  }

  private static InMemoryStorage inMemory() {
    return InMemoryStorage.create(ImmutableMap.of(ITEM, CONTENT));
  }

  private static void assertUnsupported(String method, Executable call) {
    assertThat(assertThrows(UnsupportedOperationException.class, call))
        .hasMessageThat()
        .contains("Storage." + method);
  }

  private static ReadChannel after(ReadChannel ch, ThrowingConsumer<ReadChannel> op)
      throws Throwable {
    op.accept(ch);
    return ch;
  }

  /** Reads exactly one {@code read} call into an {@code n}-byte buffer; returns the bytes read. */
  private static byte[] read(ReadChannel ch, int n) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(n);
    return copyOfRange(buffer.array(), 0, Math.max(0, ch.read(buffer)));
  }

  private static byte[] readAll(ReadChannel ch) throws IOException {
    return ByteStreams.toByteArray(Channels.newInputStream(ch));
  }
}
