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

import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.protocol.TField;
import shaded.parquet.org.apache.thrift.protocol.TList;
import shaded.parquet.org.apache.thrift.protocol.TProtocol;
import shaded.parquet.org.apache.thrift.protocol.TProtocolException;
import shaded.parquet.org.apache.thrift.protocol.TProtocolUtil;
import shaded.parquet.org.apache.thrift.protocol.TStruct;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;
import shaded.parquet.org.apache.thrift.transport.TTransportException;

class BoundedCompactProtocolTest {

  // Thrift type ids. parquet-format-structures does not bundle the TType class that defines them.
  private static final byte STRUCT = 12;
  private static final byte LIST = 15;

  @Test
  void readFileMetaData_validFooter_returnsEqualMetaData() throws TException {
    FileMetaData written = sampleFileMetaData();
    BoundedCompactProtocol protocol = protocolFor(encode(written));
    FileMetaData read = new FileMetaData();

    read.read(protocol);

    assertThat(read).isEqualTo(written);
  }

  @Test
  void readFileMetaData_schemaLongerThanMessage_throwsSizeLimitException() throws TException {
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    TProtocol out = compactProtocolWritingTo(encoded);
    out.writeStructBegin(new TStruct());
    out.writeFieldBegin(new TField("schema", LIST, (short) 2));
    out.writeListBegin(new TList(STRUCT, Integer.MAX_VALUE));
    BoundedCompactProtocol protocol = protocolFor(encoded.toByteArray());

    TProtocolException exception =
        assertThrows(TProtocolException.class, () -> new FileMetaData().read(protocol));

    assertThat(exception.getType()).isEqualTo(TProtocolException.SIZE_LIMIT);
  }

  @Test
  void readFileMetaData_createdByLongerThanMessage_throwsSizeLimitException() throws TException {
    // Field 6, created_by, as a string that declares 1000 bytes.
    BoundedCompactProtocol protocol = protocolFor(new byte[] {0x68, (byte) 0xE8, 0x07});

    TProtocolException exception =
        assertThrows(TProtocolException.class, () -> new FileMetaData().read(protocol));

    assertThat(exception.getType()).isEqualTo(TProtocolException.SIZE_LIMIT);
  }

  @Test
  void readStatistics_maxLongerThanMessage_throwsSizeLimitException() throws TException {
    // Field 1, max, as a binary value that declares 1000 bytes.
    BoundedCompactProtocol protocol = protocolFor(new byte[] {0x18, (byte) 0xE8, 0x07});

    TProtocolException exception =
        assertThrows(TProtocolException.class, () -> new Statistics().read(protocol));

    assertThat(exception.getType()).isEqualTo(TProtocolException.SIZE_LIMIT);
  }

  @Test
  void readStatistics_unknownBinaryFieldLongerThanMessage_throwsSizeLimitException()
      throws TException {
    // Field 15, which Statistics does not define, as a binary value that declares 1000 bytes.
    BoundedCompactProtocol protocol = protocolFor(new byte[] {(byte) 0xF8, (byte) 0xE8, 0x07});

    TProtocolException exception =
        assertThrows(TProtocolException.class, () -> new Statistics().read(protocol));

    assertThat(exception.getType()).isEqualTo(TProtocolException.SIZE_LIMIT);
  }

  @Test
  void readStatistics_negativeMaxLength_throwsNegativeSizeException() throws TException {
    // Field 1, max, as a binary value that declares a length of -1.
    BoundedCompactProtocol protocol =
        protocolFor(new byte[] {0x18, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x0F});

    TProtocolException exception =
        assertThrows(TProtocolException.class, () -> new Statistics().read(protocol));

    assertThat(exception.getType()).isEqualTo(TProtocolException.NEGATIVE_SIZE);
  }

  @Test
  void readStatistics_maxLongerThanRemainingBytes_throwsEndOfFileException() throws TException {
    // Field 1, max, as a binary value that declares 5 bytes, within the 10-byte message limit, but
    // no bytes follow it.
    byte[] message = {0x18, 0x05};
    BoundedCompactProtocol protocol =
        new BoundedCompactProtocol(
            new TIOStreamTransport(new ByteArrayInputStream(message)), /* messageLength= */ 10);

    TTransportException exception =
        assertThrows(TTransportException.class, () -> new Statistics().read(protocol));

    assertThat(exception.getType()).isEqualTo(TTransportException.END_OF_FILE);
  }

  @Test
  void skip_listNestedToMaximumDepth_readsWholeValue() throws TException {
    ByteArrayInputStream message =
        new ByteArrayInputStream(nestedLists(BoundedCompactProtocol.MAX_SKIP_DEPTH));
    BoundedCompactProtocol protocol =
        new BoundedCompactProtocol(new TIOStreamTransport(message), message.available());

    TProtocolUtil.skip(protocol, LIST);

    assertThat(message.available()).isEqualTo(0);
  }

  @Test
  void readFileMetaData_unknownFieldNestedPastMaximumDepth_throwsException() throws TException {
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    TProtocol out = compactProtocolWritingTo(encoded);
    out.writeStructBegin(new TStruct());
    // Field 1, version, holds an i32, so a list there is skipped as an unknown value.
    out.writeFieldBegin(new TField("", LIST, (short) 1));
    writeNestedLists(out, BoundedCompactProtocol.MAX_SKIP_DEPTH + 1);
    BoundedCompactProtocol protocol = protocolFor(encoded.toByteArray());

    TException exception = assertThrows(TException.class, () -> new FileMetaData().read(protocol));

    assertThat(exception).hasMessageThat().contains("depth");
  }

  @Test
  void constructor_negativeMessageLength_throwsIllegalArgumentException() throws TException {
    TIOStreamTransport transport = new TIOStreamTransport(new ByteArrayInputStream(new byte[0]));

    assertThrows(IllegalArgumentException.class, () -> new BoundedCompactProtocol(transport, -1));
  }

  @Test
  void constructor_nullTransport_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> new BoundedCompactProtocol(null, 0));
  }

  private static BoundedCompactProtocol protocolFor(byte[] message) throws TException {
    return new BoundedCompactProtocol(
        new TIOStreamTransport(new ByteArrayInputStream(message)), message.length);
  }

  private static TCompactProtocol compactProtocolWritingTo(ByteArrayOutputStream output)
      throws TException {
    return new TCompactProtocol(new TIOStreamTransport(output));
  }

  private static byte[] encode(FileMetaData fileMetaData) throws TException {
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    fileMetaData.write(compactProtocolWritingTo(encoded));
    return encoded.toByteArray();
  }

  /** Returns a list that holds a list, {@code depth} deep, whose innermost list is empty. */
  private static byte[] nestedLists(int depth) throws TException {
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    writeNestedLists(compactProtocolWritingTo(encoded), depth);
    return encoded.toByteArray();
  }

  private static void writeNestedLists(TProtocol out, int depth) throws TException {
    boolean hasInnerList = depth > 1;
    out.writeListBegin(new TList(LIST, hasInnerList ? 1 : 0));
    if (hasInnerList) {
      writeNestedLists(out, depth - 1);
    }
    out.writeListEnd();
  }

  /** Returns a footer with a nested schema, statistics, page encoding stats, and metadata. */
  private static FileMetaData sampleFileMetaData() {
    List<SchemaElement> schema =
        ImmutableList.of(
            new SchemaElement("root").setNum_children(2),
            new SchemaElement("id")
                .setType(Type.INT64)
                .setRepetition_type(FieldRepetitionType.REQUIRED),
            new SchemaElement("hash")
                .setType(Type.FIXED_LEN_BYTE_ARRAY)
                .setType_length(16)
                .setRepetition_type(FieldRepetitionType.OPTIONAL));
    Statistics statistics =
        new Statistics().setMin_value(new byte[0]).setMax_value(new byte[] {9, 9}).setNull_count(0);
    ColumnMetaData metaData =
        new ColumnMetaData(
                Type.INT64,
                ImmutableList.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY),
                ImmutableList.of("id"),
                CompressionCodec.SNAPPY,
                /* num_values= */ 10,
                /* total_uncompressed_size= */ 120,
                /* total_compressed_size= */ 100,
                /* data_page_offset= */ 40)
            .setDictionary_page_offset(4)
            .setStatistics(statistics)
            .setEncoding_stats(
                ImmutableList.of(
                    new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
                    new PageEncodingStats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 2)));
    RowGroup rowGroup =
        new RowGroup(
            ImmutableList.of(new ColumnChunk(/* file_offset= */ 4).setMeta_data(metaData)),
            /* total_byte_size= */ 120,
            /* num_rows= */ 10);
    return new FileMetaData(
            /* version= */ 1, schema, /* num_rows= */ 10, ImmutableList.of(rowGroup))
        .setKey_value_metadata(ImmutableList.of(new KeyValue("writer").setValue("test")))
        .setCreated_by("BoundedCompactProtocolTest");
  }
}
