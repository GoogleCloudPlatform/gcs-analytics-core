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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Util;

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
   * object, an encrypted footer, and a tail too short to contain the whole metadata block. Callers
   * are expected to treat this as "prefetching unavailable" rather than an error.
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

    try {
      byte[] metadataBytes = new byte[metadataLength];
      buffer.position(metadataIndex);
      buffer.get(metadataBytes);
      FileMetaData fileMetaData = Util.readFileMetaData(new ByteArrayInputStream(metadataBytes));
      return Optional.of(toFileLayout(fileMetaData));
    } catch (Exception e) {
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

  private static ParquetFileLayout toFileLayout(FileMetaData fileMetaData) {
    List<RowGroup> rowGroups = fileMetaData.getRow_groups();
    ImmutableList.Builder<ParquetRowGroup> parsedRowGroups = ImmutableList.builder();
    for (int rowGroupIndex = 0; rowGroupIndex < rowGroups.size(); rowGroupIndex++) {
      parsedRowGroups.add(toRowGroup(rowGroups.get(rowGroupIndex), rowGroupIndex));
    }
    return ParquetFileLayout.create(
        computeSchemaFingerprint(fileMetaData), parsedRowGroups.build());
  }

  private static ParquetRowGroup toRowGroup(RowGroup rowGroup, int rowGroupIndex) {
    ParquetRowGroup.Builder parsedRowGroup = ParquetRowGroup.builder().setIndex(rowGroupIndex);
    for (ColumnChunk columnChunk : rowGroup.getColumns()) {
      toColumnChunk(columnChunk).ifPresent(parsedRowGroup::addColumnChunk);
    }
    return parsedRowGroup.build();
  }

  private static Optional<ParquetColumnChunk> toColumnChunk(ColumnChunk columnChunk) {
    ColumnMetaData metaData = columnChunk.getMeta_data();
    if (metaData == null || metaData.getPath_in_schema() == null) {
      return Optional.empty();
    }

    long dataPageOffset = metaData.getData_page_offset();
    long dictionaryPageOffset = metaData.getDictionary_page_offset();
    boolean hasDictionaryOffset = dictionaryPageOffset > 0 && dictionaryPageOffset < dataPageOffset;
    long startOffset = hasDictionaryOffset ? dictionaryPageOffset : dataPageOffset;
    // Chunks that fall back to plain data pages mid-chunk cannot be filtered using the dictionary
    // page alone, so expose a dictionary page only when every data page is dictionary encoded.
    boolean exclusivelyDictionaryEncoded =
        hasDictionaryOffset && areAllDataPagesDictionaryEncoded(metaData);

    ParquetColumnChunk.Builder parsedChunk =
        ParquetColumnChunk.builder()
            .setColumnPath(String.join(".", metaData.getPath_in_schema()))
            .setStartOffset(startOffset)
            .setCompressedSize(metaData.getTotal_compressed_size())
            .setDataPageOffset(exclusivelyDictionaryEncoded ? dataPageOffset : startOffset);
    if (exclusivelyDictionaryEncoded) {
      parsedChunk.setDictionaryPageOffset(dictionaryPageOffset);
    }
    return Optional.of(parsedChunk.build());
  }

  private static boolean areAllDataPagesDictionaryEncoded(ColumnMetaData metaData) {
    if (!metaData.isSetEncoding_stats()) {
      return true;
    }
    for (PageEncodingStats stat : metaData.getEncoding_stats()) {
      PageType pageType = stat.getPage_type();
      if ((pageType == PageType.DATA_PAGE || pageType == PageType.DATA_PAGE_V2)
          && stat.getCount() > 0
          && !isDictionaryEncoding(stat.getEncoding())) {
        return false;
      }
    }
    return true;
  }

  private static boolean isDictionaryEncoding(Encoding encoding) {
    return encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY;
  }

  /**
   * Derives a fingerprint from the schema elements rather than the columns of a row group, so that
   * files sharing a schema agree on the fingerprint even when they contain no row groups.
   */
  private static int computeSchemaFingerprint(FileMetaData fileMetaData) {
    StringBuilder schemaSignature = new StringBuilder();
    for (SchemaElement element : fileMetaData.getSchema()) {
      schemaSignature.append(element.getName()).append(':');
      if (element.isSetType()) {
        schemaSignature.append(element.getType().getValue());
      }
      schemaSignature.append(';');
    }
    return schemaSignature.toString().hashCode();
  }
}
