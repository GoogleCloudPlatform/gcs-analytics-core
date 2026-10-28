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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.protocol.TField;
import shaded.parquet.org.apache.thrift.protocol.TList;
import shaded.parquet.org.apache.thrift.protocol.TMap;
import shaded.parquet.org.apache.thrift.protocol.TProtocol;
import shaded.parquet.org.apache.thrift.protocol.TProtocolException;
import shaded.parquet.org.apache.thrift.protocol.TProtocolUtil;
import shaded.parquet.org.apache.thrift.protocol.TSet;
import shaded.parquet.org.apache.thrift.protocol.TStruct;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

class BoundedCompactProtocolTest {

  // Thrift type ids. parquet-format-structures does not bundle the TType class that defines them.
  private static final byte I32 = 8;
  private static final byte STRUCT = 12;
  private static final byte MAP = 13;
  private static final byte SET = 14;
  private static final byte LIST = 15;

  @ParameterizedTest
  @ValueSource(bytes = {STRUCT, LIST, SET, MAP})
  void skip_valueNestedToMaximumDepth_readsWholeValue(byte type) throws TException {
    ByteArrayInputStream message =
        new ByteArrayInputStream(nestedValue(type, BoundedCompactProtocol.MAX_NESTING_DEPTH));

    TProtocolUtil.skip(protocolFor(message), type);

    assertThat(message.available()).isEqualTo(0);
  }

  @ParameterizedTest
  @ValueSource(bytes = {STRUCT, LIST, SET, MAP})
  void skip_valueNestedPastMaximumDepth_throwsDepthLimitException(byte type) throws TException {
    ByteArrayInputStream message =
        new ByteArrayInputStream(nestedValue(type, BoundedCompactProtocol.MAX_NESTING_DEPTH + 1));
    BoundedCompactProtocol protocol = protocolFor(message);

    TProtocolException exception =
        assertThrows(TProtocolException.class, () -> TProtocolUtil.skip(protocol, type));

    assertThat(exception.getType()).isEqualTo(TProtocolException.DEPTH_LIMIT);
  }

  @Test
  void skip_structWithMoreSiblingSetsThanMaximumDepth_readsWholeStruct() throws TException {
    ByteArrayInputStream message =
        new ByteArrayInputStream(structOfEmptySets(2 * BoundedCompactProtocol.MAX_NESTING_DEPTH));

    TProtocolUtil.skip(protocolFor(message), STRUCT);

    assertThat(message.available()).isEqualTo(0);
  }

  @Test
  void readListBegin_listLongerThanMessage_throwsSizeLimitException() throws TException {
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    compactProtocolWritingTo(encoded).writeListBegin(new TList(I32, 1_000));
    BoundedCompactProtocol protocol = protocolFor(new ByteArrayInputStream(encoded.toByteArray()));

    TProtocolException exception = assertThrows(TProtocolException.class, protocol::readListBegin);

    assertThat(exception.getType()).isEqualTo(TProtocolException.SIZE_LIMIT);
  }

  @Test
  void readString_stringLongerThanMessage_throwsSizeLimitException() throws TException {
    // A string length of 1000 as a varint, without the string bytes that should follow it.
    byte[] encoded = {(byte) 0xE8, 0x07};
    BoundedCompactProtocol protocol = protocolFor(new ByteArrayInputStream(encoded));

    TProtocolException exception = assertThrows(TProtocolException.class, protocol::readString);

    assertThat(exception.getType()).isEqualTo(TProtocolException.SIZE_LIMIT);
  }

  private static BoundedCompactProtocol protocolFor(ByteArrayInputStream message)
      throws TException {
    return new BoundedCompactProtocol(new TIOStreamTransport(message), message.available());
  }

  private static TCompactProtocol compactProtocolWritingTo(ByteArrayOutputStream output)
      throws TException {
    return new TCompactProtocol(new TIOStreamTransport(output));
  }

  /** Returns a value of {@code type} that holds a value of the same type, {@code depth} deep. */
  private static byte[] nestedValue(byte type, int depth) throws TException {
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    writeNestedValue(compactProtocolWritingTo(encoded), type, depth);
    return encoded.toByteArray();
  }

  private static void writeNestedValue(TProtocol out, byte type, int depth) throws TException {
    boolean hasInnerValue = depth > 1;
    int size = hasInnerValue ? 1 : 0;
    switch (type) {
      case STRUCT:
        out.writeStructBegin(new TStruct());
        if (hasInnerValue) {
          out.writeFieldBegin(new TField("", STRUCT, (short) 1));
          writeNestedValue(out, type, depth - 1);
          out.writeFieldEnd();
        }
        out.writeFieldStop();
        out.writeStructEnd();
        return;
      case LIST:
        out.writeListBegin(new TList(LIST, size));
        if (hasInnerValue) {
          writeNestedValue(out, type, depth - 1);
        }
        out.writeListEnd();
        return;
      case SET:
        out.writeSetBegin(new TSet(SET, size));
        if (hasInnerValue) {
          writeNestedValue(out, type, depth - 1);
        }
        out.writeSetEnd();
        return;
      case MAP:
        out.writeMapBegin(new TMap(I32, MAP, size));
        if (hasInnerValue) {
          out.writeI32(0);
          writeNestedValue(out, type, depth - 1);
        }
        out.writeMapEnd();
        return;
      default:
        throw new IllegalArgumentException("Unsupported type " + type);
    }
  }

  /** Returns a struct whose {@code count} fields are each an empty set. */
  private static byte[] structOfEmptySets(int count) throws TException {
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    TProtocol out = compactProtocolWritingTo(encoded);
    out.writeStructBegin(new TStruct());
    for (int fieldId = 1; fieldId <= count; fieldId++) {
      out.writeFieldBegin(new TField("", SET, (short) fieldId));
      out.writeSetBegin(new TSet(I32, 0));
      out.writeSetEnd();
      out.writeFieldEnd();
    }
    out.writeFieldStop();
    out.writeStructEnd();
    return encoded.toByteArray();
  }
}
