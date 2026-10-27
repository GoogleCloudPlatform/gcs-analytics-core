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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;

class ParquetFooterParserTest {

  private static final String CRAFTED_COLUMN = "crafted";
  private static final long CRAFTED_DICTIONARY_PAGE_OFFSET = 4;
  private static final long CRAFTED_DATA_PAGE_OFFSET = 24;
  private static final long CRAFTED_CHUNK_SIZE = 100;
  private static final byte[] PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

  /**
   * Metadata whose schema field declares a list of {@code Integer.MAX_VALUE} structs: field 2 as a
   * list, a header announcing struct elements with a separate size, then that size as a varint.
   */
  private static final byte[] HUGE_LIST_METADATA = {
    0x29, (byte) 0xFC, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07
  };

  /** Deep enough to overflow a default thread stack if decoding followed every level. */
  private static final int STACK_OVERFLOWING_DEPTH = 100_000;

  @Test
  void parse_validParquetFile_returnsLayout() throws IOException {
    byte[] fileBytes = ParquetGoldenFiles.read(ParquetGoldenFiles.SINGLE_ROW_GROUP);

    Optional<ParquetFileLayout> layout = parseWholeFile(fileBytes);

    assertThat(layout).isPresent();
  }

  @Test
  void parse_validParquetFile_extractsAllColumnPaths() throws IOException {
    ParquetFileLayout layout = parseGoldenFile(ParquetGoldenFiles.SINGLE_ROW_GROUP);

    assertThat(layout.getRowGroups().get(0).getColumnChunks().keySet())
        .containsExactly(
            ParquetGoldenFiles.ID_COLUMN,
            ParquetGoldenFiles.CATEGORY_COLUMN,
            ParquetGoldenFiles.VALUE_COLUMN)
        .inOrder();
  }

  @Test
  void parse_columnChunkOffsets_matchReferenceImplementation() throws IOException {
    ParquetFileLayout layout = parseGoldenFile(ParquetGoldenFiles.SINGLE_ROW_GROUP);

    assertThat(startOffsetsOf(layout))
        .containsExactlyElementsIn(ParquetGoldenFiles.SINGLE_ROW_GROUP_START_OFFSETS)
        .inOrder();
  }

  @Test
  void parse_columnChunkSizes_matchReferenceImplementation() throws IOException {
    ParquetFileLayout layout = parseGoldenFile(ParquetGoldenFiles.SINGLE_ROW_GROUP);

    assertThat(compressedSizesOf(layout))
        .containsExactlyElementsIn(ParquetGoldenFiles.SINGLE_ROW_GROUP_COMPRESSED_SIZES)
        .inOrder();
  }

  @Test
  void parse_multipleRowGroups_matchReferenceRowGroupOffsets() throws IOException {
    ParquetFileLayout layout = parseGoldenFile(ParquetGoldenFiles.MULTI_ROW_GROUP);

    assertThat(rowGroupStartOffsetsOf(layout))
        .containsExactlyElementsIn(ParquetGoldenFiles.MULTI_ROW_GROUP_START_OFFSETS)
        .inOrder();
  }

  @Test
  void parse_rowGroupIndices_areSequential() throws IOException {
    ParquetFileLayout layout = parseGoldenFile(ParquetGoldenFiles.MULTI_ROW_GROUP);

    assertThat(indicesOf(layout)).containsExactly(0, 1, 2, 3).inOrder();
  }

  @Test
  void parse_dictionaryEncodedColumn_exposesReferenceDictionaryPageRange() throws IOException {
    ParquetFileLayout layout = parseGoldenFile(ParquetGoldenFiles.SINGLE_ROW_GROUP);

    assertThat(chunkOf(layout, ParquetGoldenFiles.CATEGORY_COLUMN).getDictionaryPageRange())
        .hasValue(ParquetGoldenFiles.SINGLE_ROW_GROUP_CATEGORY_DICTIONARY_PAGE);
  }

  @Test
  void parse_dictionaryPageRange_endsWhereDataPagesBegin() throws IOException {
    ParquetFileLayout layout = parseGoldenFile(ParquetGoldenFiles.SINGLE_ROW_GROUP);

    ParquetColumnChunk chunk = chunkOf(layout, ParquetGoldenFiles.CATEGORY_COLUMN);

    assertThat(chunk.getDictionaryPageRange().get().upperEndpoint())
        .isEqualTo(chunk.getDataPageOffset());
  }

  @Test
  void parse_columnChunkWithoutDictionaryPage_omitsDictionaryPageRange() throws IOException {
    ParquetFileLayout layout = parseGoldenFile(ParquetGoldenFiles.DICTIONARY_FALLBACK);

    ParquetColumnChunk idChunk = chunkOf(layout, ParquetGoldenFiles.ID_COLUMN);

    assertThat(idChunk.getDictionaryPageRange()).isEmpty();
  }

  @Test
  void parse_columnChunkThatFellBackToPlainPages_omitsDictionaryPageRange() throws IOException {
    ParquetFileLayout layout = parseGoldenFile(ParquetGoldenFiles.DICTIONARY_FALLBACK);

    ParquetColumnChunk categoryChunk = chunkOf(layout, ParquetGoldenFiles.CATEGORY_COLUMN);

    assertThat(categoryChunk.getDictionaryPageRange()).isEmpty();
  }

  @Test
  void parse_columnChunkThatFellBackToPlainPages_startsDataPagesAtChunkStart() throws IOException {
    ParquetFileLayout layout = parseGoldenFile(ParquetGoldenFiles.DICTIONARY_FALLBACK);

    ParquetColumnChunk categoryChunk = chunkOf(layout, ParquetGoldenFiles.CATEGORY_COLUMN);

    assertThat(categoryChunk.getDataPageOffset()).isEqualTo(categoryChunk.getStartOffset());
  }

  @Test
  void parse_columnChunkDictionaryEncodedThroughout_exposesDictionaryPageRange()
      throws IOException {
    ParquetFileLayout layout = parseGoldenFile(ParquetGoldenFiles.DICTIONARY_FALLBACK);

    ParquetColumnChunk valueChunk = chunkOf(layout, ParquetGoldenFiles.VALUE_COLUMN);

    assertThat(valueChunk.getDictionaryPageRange()).isPresent();
  }

  @Test
  void parse_identicalSchemas_produceEqualFingerprints() throws IOException {
    long singleRowGroupFingerprint = fingerprintOf(ParquetGoldenFiles.SINGLE_ROW_GROUP);

    long multiRowGroupFingerprint = fingerprintOf(ParquetGoldenFiles.MULTI_ROW_GROUP);

    assertThat(singleRowGroupFingerprint).isEqualTo(multiRowGroupFingerprint);
  }

  @Test
  void parse_differentSchemas_produceDifferentFingerprints() throws IOException {
    long recordFingerprint = fingerprintOf(ParquetGoldenFiles.SINGLE_ROW_GROUP);

    long alternateFingerprint = fingerprintOf(ParquetGoldenFiles.ALTERNATE_SCHEMA);

    assertThat(recordFingerprint).isNotEqualTo(alternateFingerprint);
  }

  @Test
  void parse_tailContainingOnlyFooter_returnsLayout() throws IOException {
    byte[] fileBytes = ParquetGoldenFiles.read(ParquetGoldenFiles.SINGLE_ROW_GROUP);
    int tailLength = fileBytes.length - footerStartOffsetOf(fileBytes);

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(tailOf(fileBytes, tailLength));

    assertThat(layout).isPresent();
  }

  @Test
  void parse_tailTruncatedBeforeMetadata_returnsEmpty() throws IOException {
    byte[] fileBytes = ParquetGoldenFiles.read(ParquetGoldenFiles.SINGLE_ROW_GROUP);
    int tooShortLength = fileBytes.length - footerStartOffsetOf(fileBytes) - 1;

    Optional<ParquetFileLayout> layout =
        ParquetFooterParser.parse(tailOf(fileBytes, tooShortLength));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_bufferSlicedFromLargerArray_returnsSameLayout() throws IOException {
    byte[] fileBytes = ParquetGoldenFiles.read(ParquetGoldenFiles.SINGLE_ROW_GROUP);
    int padding = 16;
    byte[] paddedBytes = new byte[padding + fileBytes.length];
    System.arraycopy(fileBytes, 0, paddedBytes, padding, fileBytes.length);
    ByteBuffer slice = ByteBuffer.wrap(paddedBytes, padding, fileBytes.length).slice();

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(slice);

    assertThat(layout).hasValue(parseWholeFile(fileBytes).get());
  }

  @Test
  void parse_directBuffer_returnsSameLayout() throws IOException {
    byte[] fileBytes = ParquetGoldenFiles.read(ParquetGoldenFiles.SINGLE_ROW_GROUP);
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(fileBytes.length);
    directBuffer.put(fileBytes).flip();

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(directBuffer);

    assertThat(layout).hasValue(parseWholeFile(fileBytes).get());
  }

  @Test
  void parse_bytesWithoutParquetMagic_returnsEmpty() {
    byte[] notParquet = "this is definitely not a parquet file".getBytes(StandardCharsets.UTF_8);

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(ByteBuffer.wrap(notParquet));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_encryptedFooter_returnsEmpty() throws IOException {
    byte[] fileBytes = ParquetGoldenFiles.read(ParquetGoldenFiles.SINGLE_ROW_GROUP);
    // Objects with an encrypted footer end with the magic "PARE" instead of "PAR1".
    fileBytes[fileBytes.length - 1] = 'E';

    Optional<ParquetFileLayout> layout = parseWholeFile(fileBytes);

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_bufferShorterThanTrailer_returnsEmpty() {
    byte[] tooShort = {'P', 'A', 'R', '1'};

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(ByteBuffer.wrap(tooShort));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_corruptedMetadataBytes_returnsEmpty() throws IOException {
    byte[] fileBytes = ParquetGoldenFiles.read(ParquetGoldenFiles.SINGLE_ROW_GROUP);
    fileBytes[footerStartOffsetOf(fileBytes) + 1] ^= (byte) 0xFF;

    Optional<ParquetFileLayout> corruptedLayout = parseWholeFile(fileBytes);

    assertThat(corruptedLayout).isEmpty();
  }

  @Test
  void parse_metadataDeclaringHugeList_returnsEmpty() {
    ByteBuffer tail = withTrailer(HUGE_LIST_METADATA);

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(tail);

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_metadataNestedDeeperThanStackAllows_returnsEmpty() {
    ByteBuffer tail = withTrailer(deeplyNestedMetadata(STACK_OVERFLOWING_DEPTH));

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(tail);

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_doesNotConsumeCallerBuffer() throws IOException {
    ByteBuffer buffer =
        ByteBuffer.wrap(ParquetGoldenFiles.read(ParquetGoldenFiles.SINGLE_ROW_GROUP));

    Optional<ParquetFileLayout> unused = ParquetFooterParser.parse(buffer);

    assertThat(buffer.position()).isEqualTo(0);
  }

  @Test
  void parse_negativeMetadataLength_returnsEmpty() {
    ByteBuffer tail = withTrailer(new byte[1], /* declaredMetadataLength= */ -1);

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(tail);

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_columnChunkWithoutMetaData_returnsEmpty() throws IOException {
    ColumnChunk withoutMetaData = new ColumnChunk(/* file_offset= */ 0);

    Optional<ParquetFileLayout> layout =
        parseCrafted(withoutMetaData, columnChunkOf(craftedMetaData()));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_overlappingColumnChunks_returnsEmpty() throws IOException {
    // Both chunks span [24, 124).
    ColumnChunk first = columnChunkOf(craftedMetaData());
    ColumnChunk second =
        columnChunkOf(craftedMetaData().setPath_in_schema(ImmutableList.of("other")));

    Optional<ParquetFileLayout> layout = parseCrafted(first, second);

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_columnChunkStoredInAnotherFile_returnsEmpty() throws IOException {
    ColumnChunk external = columnChunkOf(craftedMetaData()).setFile_path("part-00000.parquet");

    Optional<ParquetFileLayout> layout = parseCrafted(external);

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_columnPathsJoiningToSameDottedPath_returnsEmpty() throws IOException {
    ColumnChunk dottedName =
        columnChunkOf(craftedMetaData().setPath_in_schema(ImmutableList.of("a.b")));
    ColumnChunk nestedName =
        columnChunkOf(
            craftedMetaData()
                .setPath_in_schema(ImmutableList.of("a", "b"))
                .setData_page_offset(CRAFTED_DATA_PAGE_OFFSET + CRAFTED_CHUNK_SIZE));

    Optional<ParquetFileLayout> layout = parseCrafted(dottedName, nestedName);

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_dataPagesStartingPastChunkEnd_returnsEmpty() throws IOException {
    // The chunk spans [4, 104), so data pages at 200 lie outside it.
    ColumnMetaData metaData = craftedDictionaryEncodedMetaData().setData_page_offset(200);

    Optional<ParquetFileLayout> layout = parseCrafted(columnChunkOf(metaData));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_plainFallbackWithDataPagesStartingPastChunkEnd_returnsEmpty() throws IOException {
    // The dictionary page cannot filter this chunk, but its data pages at 200 still lie outside
    // [4, 104).
    ColumnMetaData metaData =
        craftedDictionaryEncodedMetaData()
            .setEncodings(ImmutableList.of(Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, Encoding.RLE))
            .setData_page_offset(200);

    Optional<ParquetFileLayout> layout = parseCrafted(columnChunkOf(metaData));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_negativeCompressedSize_returnsEmpty() throws IOException {
    ColumnMetaData metaData = craftedMetaData().setTotal_compressed_size(-1);

    Optional<ParquetFileLayout> layout = parseCrafted(columnChunkOf(metaData));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_chunkEndingBeyondLongRange_returnsEmpty() throws IOException {
    ColumnMetaData metaData = craftedMetaData().setData_page_offset(Long.MAX_VALUE - 10);

    Optional<ParquetFileLayout> layout = parseCrafted(columnChunkOf(metaData));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_chunkStartingInsideLeadingMagic_returnsEmpty() throws IOException {
    ColumnMetaData metaData = craftedMetaData().setData_page_offset(2);

    Optional<ParquetFileLayout> layout = parseCrafted(columnChunkOf(metaData));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_dictionaryPageOffsetAfterDataPages_startsChunkAtDataPageOffset() throws IOException {
    ColumnMetaData metaData =
        craftedMetaData().setDictionary_page_offset(CRAFTED_DATA_PAGE_OFFSET + 1);

    ParquetColumnChunk chunk = parseCraftedChunk(metaData);

    assertThat(chunk.getStartOffset()).isEqualTo(CRAFTED_DATA_PAGE_OFFSET);
  }

  @Test
  void parse_dictionaryEncodedColumnWithoutEncodingStats_exposesDictionaryPageRange()
      throws IOException {
    ParquetColumnChunk chunk = parseCraftedChunk(craftedDictionaryEncodedMetaData());

    assertThat(chunk.getDictionaryPageRange())
        .hasValue(Range.closedOpen(CRAFTED_DICTIONARY_PAGE_OFFSET, CRAFTED_DATA_PAGE_OFFSET));
  }

  @Test
  void parse_plainFallbackWithoutEncodingStats_omitsDictionaryPageRange() throws IOException {
    ColumnMetaData metaData =
        craftedDictionaryEncodedMetaData()
            .setEncodings(
                ImmutableList.of(Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, Encoding.RLE));

    ParquetColumnChunk chunk = parseCraftedChunk(metaData);

    assertThat(chunk.getDictionaryPageRange()).isEmpty();
  }

  private static ParquetFileLayout parseGoldenFile(String fileName) throws IOException {
    return parseWholeFile(ParquetGoldenFiles.read(fileName)).get();
  }

  private static Optional<ParquetFileLayout> parseWholeFile(byte[] fileBytes) {
    return ParquetFooterParser.parse(ByteBuffer.wrap(fileBytes));
  }

  /** Returns the offset of the footer metadata, read from the 4-byte length before the magic. */
  private static int footerStartOffsetOf(byte[] fileBytes) {
    int trailerLength = 8;
    int metadataLength =
        ByteBuffer.wrap(fileBytes)
            .order(ByteOrder.LITTLE_ENDIAN)
            .getInt(fileBytes.length - trailerLength);
    return fileBytes.length - trailerLength - metadataLength;
  }

  private static ByteBuffer tailOf(byte[] fileBytes, int tailLength) {
    return ByteBuffer.wrap(fileBytes, fileBytes.length - tailLength, tailLength);
  }

  private static long fingerprintOf(String fileName) throws IOException {
    return parseGoldenFile(fileName).getSchemaFingerprint();
  }

  private static ParquetColumnChunk chunkOf(ParquetFileLayout layout, String columnPath) {
    return layout.getRowGroups().get(0).getColumnChunk(columnPath).get();
  }

  private static List<Long> startOffsetsOf(ParquetFileLayout layout) {
    List<Long> offsets = new ArrayList<>();
    for (ParquetRowGroup rowGroup : layout.getRowGroups()) {
      for (ParquetColumnChunk chunk : rowGroup.getColumnChunks().values()) {
        offsets.add(chunk.getStartOffset());
      }
    }
    return offsets;
  }

  private static List<Long> compressedSizesOf(ParquetFileLayout layout) {
    List<Long> sizes = new ArrayList<>();
    for (ParquetRowGroup rowGroup : layout.getRowGroups()) {
      for (ParquetColumnChunk chunk : rowGroup.getColumnChunks().values()) {
        sizes.add(chunk.getCompressedSize());
      }
    }
    return sizes;
  }

  private static List<Long> rowGroupStartOffsetsOf(ParquetFileLayout layout) {
    List<Long> offsets = new ArrayList<>();
    for (ParquetRowGroup rowGroup : layout.getRowGroups()) {
      offsets.add(rowGroup.getStartOffset());
    }
    return offsets;
  }

  private static List<Integer> indicesOf(ParquetFileLayout layout) {
    List<Integer> indices = new ArrayList<>();
    for (ParquetRowGroup rowGroup : layout.getRowGroups()) {
      indices.add(rowGroup.getIndex());
    }
    return indices;
  }

  /**
   * Returns metadata whose unknown field 100 nests {@code depth} structs, one byte per level. The
   * structs are never closed, because decoding has to give up long before reaching the end.
   */
  private static byte[] deeplyNestedMetadata(int depth) {
    ByteArrayOutputStream metadata = new ByteArrayOutputStream();
    // A struct field whose id follows as a varint: 100, zigzag encoded.
    metadata.write(0x0C);
    metadata.write(0xC8);
    metadata.write(0x01);
    for (int level = 1; level < depth; level++) {
      // A struct field with the next id, opening one more level.
      metadata.write(0x1C);
    }
    return metadata.toByteArray();
  }

  private static ColumnMetaData craftedMetaData() {
    return new ColumnMetaData(
        Type.INT64,
        ImmutableList.of(Encoding.PLAIN),
        ImmutableList.of(CRAFTED_COLUMN),
        CompressionCodec.UNCOMPRESSED,
        /* num_values= */ 10,
        /* total_uncompressed_size= */ CRAFTED_CHUNK_SIZE,
        /* total_compressed_size= */ CRAFTED_CHUNK_SIZE,
        /* data_page_offset= */ CRAFTED_DATA_PAGE_OFFSET);
  }

  /** Returns metadata of a chunk written by a Parquet 1.0 writer that used only the dictionary. */
  private static ColumnMetaData craftedDictionaryEncodedMetaData() {
    return craftedMetaData()
        .setEncodings(ImmutableList.of(Encoding.PLAIN_DICTIONARY, Encoding.RLE))
        .setDictionary_page_offset(CRAFTED_DICTIONARY_PAGE_OFFSET);
  }

  private static ColumnChunk columnChunkOf(ColumnMetaData metaData) {
    return new ColumnChunk(/* file_offset= */ 0).setMeta_data(metaData);
  }

  private static ParquetColumnChunk parseCraftedChunk(ColumnMetaData metaData) throws IOException {
    return parseCraftedRowGroup(columnChunkOf(metaData)).getColumnChunk(CRAFTED_COLUMN).get();
  }

  private static ParquetRowGroup parseCraftedRowGroup(ColumnChunk... columns) throws IOException {
    return parseCrafted(columns).get().getRowGroups().get(0);
  }

  /**
   * Parses a footer built directly from Thrift structures, to reach layouts that real writers
   * rarely produce.
   */
  private static Optional<ParquetFileLayout> parseCrafted(ColumnChunk... columns)
      throws IOException {
    RowGroup rowGroup =
        new RowGroup(
            ImmutableList.copyOf(columns),
            /* total_byte_size= */ CRAFTED_CHUNK_SIZE,
            /* num_rows= */ 10);
    FileMetaData fileMetaData =
        new FileMetaData(
            /* version= */ 1,
            ImmutableList.of(new SchemaElement("schema")),
            /* num_rows= */ 10,
            ImmutableList.of(rowGroup));
    ByteArrayOutputStream metadata = new ByteArrayOutputStream();
    Util.writeFileMetaData(fileMetaData, metadata);
    return ParquetFooterParser.parse(withTrailer(metadata.toByteArray()));
  }

  private static ByteBuffer withTrailer(byte[] metadata) {
    return withTrailer(metadata, metadata.length);
  }

  /** Appends the little-endian metadata length and the Parquet magic to {@code metadata}. */
  private static ByteBuffer withTrailer(byte[] metadata, int declaredMetadataLength) {
    ByteBuffer tail =
        ByteBuffer.allocate(metadata.length + Integer.BYTES + PARQUET_MAGIC.length)
            .order(ByteOrder.LITTLE_ENDIAN);
    tail.put(metadata).putInt(declaredMetadataLength).put(PARQUET_MAGIC);
    tail.flip();
    return tail;
  }
}
