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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

/**
 * Decodes the tail of a Parquet object into a {@link ParquetFileLayout}.
 *
 * <p>Uses the generated Thrift model from {@code parquet-format-structures}, which bundles its own
 * shaded Thrift runtime and does not depend on Hadoop.
 */
final class ParquetFooterParser {

  private static final int MAGIC_LENGTH = 4;
  private static final int FOOTER_LENGTH_BYTES = 4;
  private static final int TRAILER_LENGTH = MAGIC_LENGTH + FOOTER_LENGTH_BYTES;
  private static final byte[] PARQUET_MAGIC = {'P', 'A', 'R', '1'};

  private ParquetFooterParser() {}

  /**
   * Parses the trailing bytes of a Parquet object.
   *
   * <p>Returns empty when the bytes are not a readable Parquet footer, which includes a non-Parquet
   * object, an encrypted footer, a tail too short to contain the whole metadata block, and metadata
   * whose column chunks cannot be mapped onto the bytes of this object. Callers are expected to
   * treat this as "prefetching unavailable" rather than an error.
   *
   * @param tail the last {@code tail.remaining()} bytes of the object
   */
  static Optional<ParquetFileLayout> parse(ByteBuffer tail) {
    checkNotNull(tail, "tail cannot be null");

    ByteBuffer buffer = tail.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    int tailLength = buffer.remaining();
    int tailStart = buffer.position();
    if (tailLength <= TRAILER_LENGTH) {
      return Optional.empty();
    }
    if (!hasParquetMagic(buffer, tailStart + tailLength - MAGIC_LENGTH)) {
      return Optional.empty();
    }

    int metadataLengthIndex = tailStart + tailLength - TRAILER_LENGTH;
    int metadataLength = buffer.getInt(metadataLengthIndex);
    int metadataIndex = metadataLengthIndex - metadataLength;
    if (metadataLength <= 0 || metadataIndex < tailStart) {
      return Optional.empty();
    }

    FileMetaData fileMetaData;
    try {
      fileMetaData = readFileMetaData(buffer, metadataIndex, metadataLength);
    } catch (TException e) {
      // Malformed metadata, including metadata that would exhaust the heap or the stack, which the
      // bounded protocol rejects. Only prefetching is lost; reading the object is unaffected.
      return Optional.empty();
    }
    try {
      return Optional.of(toFileLayout(fileMetaData));
    } catch (IllegalArgumentException e) {
      // Metadata that cannot be mapped onto the bytes of this object, which the layout model and
      // the checks below reject. Again, only prefetching is lost.
      return Optional.empty();
    }
  }

  private static boolean hasParquetMagic(ByteBuffer buffer, int magicIndex) {
    for (int i = 0; i < MAGIC_LENGTH; i++) {
      if (buffer.get(magicIndex + i) != PARQUET_MAGIC[i]) {
        return false;
      }
    }
    return true;
  }

  private static FileMetaData readFileMetaData(
      ByteBuffer buffer, int metadataIndex, int metadataLength) throws TException {
    try (TIOStreamTransport transport =
        new TIOStreamTransport(metadataStream(buffer, metadataIndex, metadataLength))) {
      FileMetaData fileMetaData = new FileMetaData();
      fileMetaData.read(new BoundedCompactProtocol(transport, metadataLength));
      return fileMetaData;
    }
  }

  /** Returns the metadata bytes as a stream, reading the backing array in place when possible. */
  private static InputStream metadataStream(
      ByteBuffer buffer, int metadataIndex, int metadataLength) {
    if (buffer.hasArray()) {
      return new ByteArrayInputStream(
          buffer.array(), buffer.arrayOffset() + metadataIndex, metadataLength);
    }
    byte[] metadataBytes = new byte[metadataLength];
    buffer.position(metadataIndex);
    buffer.get(metadataBytes);
    return new ByteArrayInputStream(metadataBytes);
  }

  private static ParquetFileLayout toFileLayout(FileMetaData fileMetaData) {
    List<RowGroup> rowGroups = fileMetaData.getRow_groups();
    ImmutableList.Builder<ParquetRowGroup> parsedRowGroups =
        ImmutableList.builderWithExpectedSize(rowGroups.size());
    for (int rowGroupIndex = 0; rowGroupIndex < rowGroups.size(); rowGroupIndex++) {
      parsedRowGroups.add(toRowGroup(rowGroups.get(rowGroupIndex), rowGroupIndex));
    }
    return ParquetFileLayout.create(
        ParquetSchemaFingerprint.of(fileMetaData.getSchema()), parsedRowGroups.build());
  }

  private static ParquetRowGroup toRowGroup(RowGroup rowGroup, int rowGroupIndex) {
    List<ParquetColumnChunk> columnChunks = new ArrayList<>(rowGroup.getColumns().size());
    for (ColumnChunk columnChunk : rowGroup.getColumns()) {
      columnChunks.add(toColumnChunk(columnChunk));
    }
    // Also rejects distinct paths such as [a.b] and [a, b], which join to the same dotted path and
    // would make the row group ambiguous.
    return ParquetRowGroup.create(rowGroupIndex, columnChunks);
  }

  /**
   * Maps a column chunk onto the bytes of this object.
   *
   * @throws IllegalArgumentException if the chunk has no metadata, its pages are stored in another
   *     file, or its offsets do not describe a consistent range of this object
   */
  private static ParquetColumnChunk toColumnChunk(ColumnChunk columnChunk) {
    // Summary files such as _metadata describe column chunks stored in other files.
    checkArgument(
        Strings.isNullOrEmpty(columnChunk.getFile_path()),
        "Column chunk is stored in another file: %s",
        columnChunk.getFile_path());
    ColumnMetaData metaData = columnChunk.getMeta_data();
    // Writers keep at least a redacted copy of the metadata of every column under a plaintext
    // footer, so a chunk without it is unusual; reject it rather than guess where its bytes are.
    checkArgument(metaData != null, "Column chunk has no metadata");
    long dataPageOffset = metaData.getData_page_offset();
    long dictionaryPageOffset = metaData.getDictionary_page_offset();
    // Like parquet-mr, treat 0 as unset and ignore a dictionary offset that does not precede the
    // data pages, which some older writers produced.
    boolean hasDictionaryOffset = dictionaryPageOffset > 0 && dictionaryPageOffset < dataPageOffset;
    long startOffset = hasDictionaryOffset ? dictionaryPageOffset : dataPageOffset;
    // No chunk can start inside the magic bytes that open every Parquet object.
    checkArgument(
        startOffset >= MAGIC_LENGTH,
        "Column chunk starts inside the leading magic: %s",
        startOffset);
    // Chunks that fall back to plain data pages mid-chunk cannot be filtered using the dictionary
    // page alone, so expose a dictionary page only when every data page is dictionary encoded.
    boolean exclusivelyDictionaryEncoded =
        hasDictionaryOffset && ParquetDictionaryEncoding.encodesEveryDataPage(metaData);
    return ParquetColumnChunk.create(
        String.join(".", metaData.getPath_in_schema()),
        startOffset,
        metaData.getTotal_compressed_size(),
        dataPageOffset,
        exclusivelyDictionaryEncoded);
  }
}
