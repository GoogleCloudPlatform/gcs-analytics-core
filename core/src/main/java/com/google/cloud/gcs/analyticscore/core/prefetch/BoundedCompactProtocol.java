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

import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.protocol.TList;
import shaded.parquet.org.apache.thrift.protocol.TMap;
import shaded.parquet.org.apache.thrift.protocol.TProtocolException;
import shaded.parquet.org.apache.thrift.protocol.TStruct;
import shaded.parquet.org.apache.thrift.transport.TTransport;

/**
 * A compact Thrift protocol that rejects messages which would exhaust the heap or the stack.
 *
 * <p>Parquet footers come from untrusted objects. Without limits, a few crafted bytes can declare a
 * list of two billion elements, which the generated code allocates up front, or nest values deeply
 * enough to overflow the stack while they are read or skipped. Both fail with an {@link Error}
 * rather than an exception. This protocol caps every string and container at the length of the
 * message, since each element occupies at least one byte, and caps nesting at {@link
 * #MAX_NESTING_DEPTH}, so such messages fail with a {@link TProtocolException} instead.
 */
final class BoundedCompactProtocol extends TCompactProtocol {

  /** The deepest nesting accepted, well above the roughly ten levels that Parquet footers use. */
  static final int MAX_NESTING_DEPTH = 64;

  private int nestingDepth;

  /**
   * Creates a protocol that reads one message of {@code messageLength} bytes from {@code
   * transport}.
   */
  BoundedCompactProtocol(TTransport transport, int messageLength) {
    super(transport, messageLength, messageLength);
  }

  @Override
  public TStruct readStructBegin() throws TException {
    enterNestedValue();
    return super.readStructBegin();
  }

  @Override
  public void readStructEnd() throws TException {
    super.readStructEnd();
    exitNestedValue();
  }

  /** Also counts sets, whose header {@link TCompactProtocol#readSetBegin()} reads through here. */
  @Override
  public TList readListBegin() throws TException {
    enterNestedValue();
    return super.readListBegin();
  }

  @Override
  public void readListEnd() throws TException {
    super.readListEnd();
    exitNestedValue();
  }

  @Override
  public void readSetEnd() throws TException {
    super.readSetEnd();
    exitNestedValue();
  }

  @Override
  public TMap readMapBegin() throws TException {
    enterNestedValue();
    return super.readMapBegin();
  }

  @Override
  public void readMapEnd() throws TException {
    super.readMapEnd();
    exitNestedValue();
  }

  private void enterNestedValue() throws TProtocolException {
    nestingDepth++;
    if (nestingDepth > MAX_NESTING_DEPTH) {
      throw new TProtocolException(
          TProtocolException.DEPTH_LIMIT, "Nesting depth exceeds " + MAX_NESTING_DEPTH);
    }
  }

  private void exitNestedValue() {
    nestingDepth--;
  }
}
