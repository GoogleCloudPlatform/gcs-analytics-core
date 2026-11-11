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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class ByteBufferInputStreamTest {

  @Test
  void read_byteAboveSignedRange_returnsUnsignedValue() {
    ByteBufferInputStream stream = streamOf((byte) 0xFF);

    int value = stream.read();

    assertThat(value).isEqualTo(255);
  }

  @Test
  void read_atEndOfBuffer_returnsMinusOne() {
    ByteBufferInputStream stream = streamOf();

    int value = stream.read();

    assertThat(value).isEqualTo(-1);
  }

  @Test
  void read_bufferPositionedPastItsStart_startsAtPosition() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3}, 1, 2);
    ByteBufferInputStream stream = new ByteBufferInputStream(buffer);

    int value = stream.read();

    assertThat(value).isEqualTo(2);
  }

  @Test
  void read_doesNotConsumeCallerBuffer() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    ByteBufferInputStream stream = new ByteBufferInputStream(buffer);

    stream.read();

    assertThat(buffer.position()).isEqualTo(0);
  }

  @Test
  void readArray_requestWithinRemainingBytes_copiesBytesAtOffset() {
    ByteBufferInputStream stream = streamOf((byte) 1, (byte) 2, (byte) 3);
    byte[] bytes = new byte[4];

    stream.read(bytes, 1, 2);

    assertThat(bytes).isEqualTo(new byte[] {0, 1, 2, 0});
  }

  @Test
  void readArray_requestLongerThanRemainingBytes_returnsRemainingLength() {
    ByteBufferInputStream stream = streamOf((byte) 1, (byte) 2, (byte) 3);

    int readLength = stream.read(new byte[8], 0, 8);

    assertThat(readLength).isEqualTo(3);
  }

  @Test
  void readArray_readOnlyBuffer_returnsRequestedLength() {
    ByteBuffer readOnlyBuffer = ByteBuffer.wrap(new byte[] {1, 2, 3}).asReadOnlyBuffer();
    ByteBufferInputStream stream = new ByteBufferInputStream(readOnlyBuffer);

    int readLength = stream.read(new byte[2], 0, 2);

    assertThat(readLength).isEqualTo(2);
  }

  @Test
  void readArray_atEndOfBuffer_returnsMinusOne() {
    ByteBufferInputStream stream = streamOf();

    int readLength = stream.read(new byte[1], 0, 1);

    assertThat(readLength).isEqualTo(-1);
  }

  @Test
  void readArray_zeroLengthAtEndOfBuffer_returnsZero() {
    ByteBufferInputStream stream = streamOf();

    int readLength = stream.read(new byte[1], 0, 0);

    assertThat(readLength).isEqualTo(0);
  }

  @Test
  void readArray_rangeOutsideArray_throwsIndexOutOfBoundsException() {
    ByteBufferInputStream stream = streamOf((byte) 1);

    assertThrows(IndexOutOfBoundsException.class, () -> stream.read(new byte[1], 1, 1));
  }

  @Test
  void constructor_nullBuffer_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> new ByteBufferInputStream(null));
  }

  private static ByteBufferInputStream streamOf(byte... bytes) {
    return new ByteBufferInputStream(ByteBuffer.wrap(bytes));
  }
}
