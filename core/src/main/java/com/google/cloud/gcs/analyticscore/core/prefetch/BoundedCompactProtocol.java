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

package com.google.cloud.gcs.analyticscore.core.prefetch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.protocol.TProtocolException;
import shaded.parquet.org.apache.thrift.protocol.TProtocolUtil;
import shaded.parquet.org.apache.thrift.transport.TTransport;

/**
 * A compact Thrift protocol that rejects messages which would exhaust the heap or the stack.
 *
 * <p>Parquet footers come from untrusted objects. Without limits, a few crafted bytes can declare a
 * list of two billion elements, which the generated code allocates up front, or nest an unknown
 * field deeply enough to overflow the stack while it is skipped. Both fail with an {@link Error}
 * rather than an exception. This protocol caps every string, binary value, and container at the
 * length of the message, since each element occupies at least one byte, and caps how deep an
 * unknown value can nest, so such messages fail with a {@link TException} instead.
 *
 * <p>Generated code skips unknown fields through {@link TProtocolUtil#skip}, whose depth limit is
 * shared by every user of the same {@code TProtocolUtil} class. Loading this class sets it to
 * {@link #MAX_SKIP_DEPTH}. The packaged jar relocates Thrift, so there the limit applies only to
 * this library's copy. On an unshaded classpath, such as unit tests or an IDE, it also applies to
 * any other library that uses the Thrift runtime of {@code parquet-format-structures}.
 *
 * <p>The cap applies to each value on its own, not to the message as a whole. The generated code
 * sizes every list before it reads the elements, so a message of {@code n} bytes can still allocate
 * one reference per declared element in each list that is open at the same time, which is a few
 * times {@code n} references for a Parquet footer. Callers must therefore bound the message length.
 *
 * <p>Each instance reads a single message and is neither reusable nor thread-safe.
 */
final class BoundedCompactProtocol extends TCompactProtocol {

  /** The deepest unknown value that can be skipped, well above the nesting Parquet footers use. */
  static final int MAX_SKIP_DEPTH = 64;

  static {
    TProtocolUtil.setMaxSkipDepth(MAX_SKIP_DEPTH);
  }

  private final int messageLength;

  /**
   * Creates a protocol that reads one message of {@code messageLength} bytes from {@code
   * transport}.
   *
   * @throws IllegalArgumentException if {@code messageLength} is negative, which Thrift would
   *     otherwise treat as no limit at all
   */
  BoundedCompactProtocol(TTransport transport, int messageLength) {
    super(checkNotNull(transport, "transport"), messageLength, messageLength);
    checkArgument(messageLength >= 0, "Message length must not be negative: %s", messageLength);
    this.messageLength = messageLength;
  }

  /**
   * Reads a binary value, rejecting a declared length that is negative or longer than the message.
   *
   * <p>{@link TCompactProtocol#readBinary()} applies neither the string limit nor a sign check, so
   * a few bytes could otherwise allocate up to the transport's limit or fail with a {@link
   * NullPointerException}.
   */
  @Override
  public ByteBuffer readBinary() throws TException {
    byte[] value = new byte[readBinaryLength()];
    getTransport().readAll(value, 0, value.length);
    return ByteBuffer.wrap(value);
  }

  /**
   * Reads the unsigned varint that precedes a binary value. {@link #readI32()} reads a zigzag
   * varint, so re-encoding its result as zigzag recovers the raw value.
   */
  private int readBinaryLength() throws TException {
    int zigzag = readI32();
    int length = (zigzag << 1) ^ (zigzag >> 31);
    if (length < 0) {
      throw new TProtocolException(
          TProtocolException.NEGATIVE_SIZE, "Negative binary length: " + length);
    }
    if (length > messageLength) {
      throw new TProtocolException(
          TProtocolException.SIZE_LIMIT,
          "Binary length " + length + " exceeds message length " + messageLength);
    }
    return length;
  }
}
