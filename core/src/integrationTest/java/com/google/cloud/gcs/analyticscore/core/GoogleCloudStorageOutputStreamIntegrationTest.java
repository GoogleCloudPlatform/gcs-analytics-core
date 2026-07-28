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

package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpStorageOptions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(named = IntegrationTestHelper.GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
@EnabledIfSystemProperty(named = IntegrationTestHelper.GCS_INTEGRATION_TEST_PROJECT_ID_PROPERTY, matches = ".+")
class GoogleCloudStorageOutputStreamIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(GoogleCloudStorageOutputStreamIntegrationTest.class);

  private static final int KB = 1024;
  private static final int MB = 1024 * KB;

  // File prefixes and extensions
  private static final String FILE_PREFIX_TXT = "test-file-txt-";
  private static final String FILE_PREFIX_CSV = "test-file-csv-";
  private static final String FILE_PREFIX_BIN = "test-file-bin-";
  private static final String FILE_PREFIX_PARQUET = "test-file-parquet-";
  private static final String SUFFIX_TXT = ".txt";
  private static final String SUFFIX_CSV = ".csv";
  private static final String SUFFIX_BIN = ".bin";
  private static final String SUFFIX_PARQUET = ".parquet";

  // Shared test payloads
  private static final byte[] TEST_CONTENT =
      "Hello, GCS Analytics Core!".getBytes(StandardCharsets.UTF_8);
  private static final byte[] CSV_CONTENT =
      "id,name,city\n1,Alice,NYC\n2,Bob,SFO\n".getBytes(StandardCharsets.UTF_8);
  private static final byte[] FIRST_WRITE_CONTENT =
      "First write".getBytes(StandardCharsets.UTF_8);
  private static final byte[] SECOND_WRITE_CONTENT =
      "Second write attempts to overwrite".getBytes(StandardCharsets.UTF_8);
  private static final byte[] ORIGINAL_CONTENT =
      "Original Version".getBytes(StandardCharsets.UTF_8);
  private static final byte[] CONCURRENT_CONTENT =
      "Concurrent Overwrite".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STALE_CONTENT =
      "Stale write attempting to overwrite".getBytes(StandardCharsets.UTF_8);

  private static final byte[] CHUNK1_CONTENT = "Hello ".getBytes(StandardCharsets.UTF_8);
  private static final byte[] CHUNK2_CONTENT = "World!".getBytes(StandardCharsets.UTF_8);

  private static final String CSEK_RAW_SECRET = "Top secret encrypted content!";
  private static final byte[] CSEK_ENCRYPTED_CONTENT =
      CSEK_RAW_SECRET.getBytes(StandardCharsets.UTF_8);
  private static final String CSEK_KEY = "MDEyMzQ1Njc4OUFCQ0RFRkdISUpLTE1OT1BRUlNUVVU=";

  // Parquet configuration constants
  private static final String PARQUET_SCHEMA_STRING =
      "message test { required binary name (UTF8); }";
  private static final String PARQUET_FIELD_NAME = "name";
  private static final String PARQUET_VAL_ALICE = "Alice";
  private static final String PARQUET_VAL_BOB = "Bob";

  private static final String FILE_PREFIX_CTAS = "ctas_output-";

  // Error messages and configurations
  private static final String GENERATION_MISMATCH_MESSAGE = "Generation mismatch for object";
  private static final String NON_EXISTENT_BUCKET = "non-existent-bucket-" + UUID.randomUUID();
  private static final String NON_EXISTENT_FILE = "test-file.txt";

  private GcsFileSystem gcsFileSystem;
  private List<BlobId> blobsToDelete;

  @BeforeEach
  void setUp() throws IOException {
    GcsFileSystemOptions options = GcsFileSystemOptions.builder()
        .setGcsClientOptions(GcsClientOptions.builder().build())
        .build();
    gcsFileSystem = new GcsFileSystemImpl(options);
    blobsToDelete = new ArrayList<>();
  }

  @AfterEach
  void tearDown() {
    if (IntegrationTestHelper.storage != null) {
      for (BlobId blobId : blobsToDelete) {
        try {
          IntegrationTestHelper.storage.delete(blobId);
        } catch (Exception e) {
          logger.warn("Failed to delete blob during cleanup: {}", blobId, e);
        }
      }
    }
  }

  private TestFileContext createTestFileContext(String prefix, String suffix) {
    String fileName = getRandomFileName(prefix, suffix);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    GcsItemId itemId = GcsItemId.builder().setBucketName(blobId.getBucket()).setObjectName(blobId.getName()).build();
    return new TestFileContext(fileName, uri, itemId);
  }

  private static final class TestFileContext {
    final String fileName;
    final URI uri;
    final GcsItemId itemId;

    TestFileContext(String fileName, URI uri, GcsItemId itemId) {
      this.fileName = fileName;
      this.uri = uri;
      this.itemId = itemId;
    }
  }

  private String getRandomFileName(String prefix, String suffix) {
    return prefix + UUID.randomUUID() + suffix;
  }

  private static Stream<Arguments> provideContentForWrite() {
    return Stream.of(
        Arguments.of(TEST_CONTENT, FILE_PREFIX_TXT, SUFFIX_TXT),
        Arguments.of(CSV_CONTENT, FILE_PREFIX_CSV, SUFFIX_CSV),
        Arguments.of(new byte[0], FILE_PREFIX_TXT, SUFFIX_TXT)
    );
  }

  @ParameterizedTest
  @MethodSource("provideContentForWrite")
  void write_variousContentTypes_createsFileSuccessfully(byte[] content, String prefix, String suffix) throws IOException {
    TestFileContext ctx = createTestFileContext(prefix, suffix);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(createFileSystemWithWriteOptions(writeOptions), ctx.itemId)) {
      outputStream.write(content);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(ctx.fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(ctx.uri).getItemInfo().getSize()).isEqualTo((long) content.length);
  }

  @Test
  void write_withChecksumValidationEnabled_createsFileSuccessfully() throws IOException {
    TestFileContext ctx = createTestFileContext(FILE_PREFIX_TXT, SUFFIX_TXT);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder()
        .setChecksumValidationEnabled(true)
        .build();

    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(createFileSystemWithWriteOptions(writeOptions), ctx.itemId)) {
      outputStream.write(TEST_CONTENT);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(ctx.fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(ctx.uri).getItemInfo().getSize()).isEqualTo((long) TEST_CONTENT.length);
  }

  @Test
  void write_largeFileWithMultipleChunks_createsFileSuccessfully() throws IOException {
    TestFileContext ctx = createTestFileContext(FILE_PREFIX_BIN, SUFFIX_BIN);
    GcsClientOptions clientOptions = GcsClientOptions.builder()
        .setUploadChunkSize(256 * KB)
        .build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    int totalSize = MB;
    byte[] chunk = new byte[KB];

    GcsFileSystem customFs = createFileSystemWithClientOptions(clientOptions);
    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(customFs, ctx.itemId)) {
      for (int i = 0; i < totalSize / chunk.length; i++) {
        outputStream.write(chunk);
      }
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(ctx.fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(ctx.uri).getItemInfo().getSize()).isEqualTo((long) totalSize);
  }

  @Test
  void write_toDiskThenUploadEnabled_createsFileSuccessfully() throws IOException {
    TestFileContext ctx = createTestFileContext(FILE_PREFIX_TXT, SUFFIX_TXT);
    GcsClientOptions clientOptions = GcsClientOptions.builder()
        .setUploadType(GcsClientOptions.UploadType.WRITE_TO_DISK_THEN_UPLOAD)
        .build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

    GcsFileSystem customFs = createFileSystemWithClientOptions(clientOptions);
    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(customFs, ctx.itemId)) {
      outputStream.write(TEST_CONTENT);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(ctx.fileName)).isTrue();
    assertThat(customFs.getFileInfo(ctx.uri).getItemInfo().getSize()).isEqualTo((long) TEST_CONTENT.length);
  }

  @Test
  void write_withJournalingEnabled_throwsUnsupportedOperationException() {
    GcsClientOptions clientOptions = GcsClientOptions.builder()
        .setUploadType(GcsClientOptions.UploadType.JOURNALING)
        .setTemporaryPaths(Collections.singletonList("/tmp/dummy-path"))
        .build();
    assertThrows(UnsupportedOperationException.class, () -> createFileSystemWithClientOptions(clientOptions));
  }

  @Test
  void write_parquetContent_createsFileSuccessfully() throws IOException {
    TestFileContext ctx = createTestFileContext(FILE_PREFIX_PARQUET, SUFFIX_PARQUET);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
      GcsFileSystemOptions readFsOptions = GcsFileSystemOptions.createFromOptions(Map.of(), "gcs.");
    InputFile inputFile = new TestInputStreamInputFile(ctx.uri, false, readFsOptions);
    List<String> namesRead = new ArrayList<>();
    Configuration conf = new Configuration();

    // Use the GCS file system to write Parquet data.
    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(createFileSystemWithWriteOptions(writeOptions), ctx.itemId)) {
      OutputFile outputFile = new TestOutputStreamOutputFile(outputStream);
      MessageType schema = MessageTypeParser.parseMessageType(PARQUET_SCHEMA_STRING);
      GroupWriteSupport.setSchema(schema, conf);
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
          .withConf(conf)
          .build()) {
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        writer.write(groupFactory.newGroup().append(PARQUET_FIELD_NAME, PARQUET_VAL_ALICE));
        writer.write(groupFactory.newGroup().append(PARQUET_FIELD_NAME, PARQUET_VAL_BOB));
      }
    }
    // Read the written content for verifying the correctness of the data written.
    try (ParquetReader<Group> reader = new GroupParquetReaderBuilder(inputFile).withConf(conf).build()) {
      Group group;
      while ((group = reader.read()) != null) {
        namesRead.add(group.getString(PARQUET_FIELD_NAME, 0));
      }
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(ctx.fileName)).isTrue();
    assertThat(namesRead).containsExactly(PARQUET_VAL_ALICE, PARQUET_VAL_BOB).inOrder();
  }

  @Test
  void copyParquetFile_fromExistingFile_writesCorrectly() throws IOException {
    IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
    URI sourceUri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE);
    GcsFileSystemOptions readFsOptions = GcsFileSystemOptions.createFromOptions(Map.of(), "gcs.");
    InputFile inputFile = new TestInputStreamInputFile(sourceUri, false, readFsOptions);
    ParquetMetadata metadata =
        ParquetHelper.readParquetMetadata(sourceUri, readFsOptions);
    MessageType schema = metadata.getFileMetaData().getSchema();
    TestFileContext destCtx = createTestFileContext(FILE_PREFIX_CTAS, SUFFIX_PARQUET);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    int recordsCopied = 0;
    Configuration conf = new Configuration();
    List<String> sourceRecordSignatures = new ArrayList<>();

    // Copy Parquet file from existing file.
    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(createFileSystemWithWriteOptions(writeOptions), destCtx.itemId)) {
      OutputFile outputFile = new TestOutputStreamOutputFile(outputStream);
      GroupWriteSupport.setSchema(schema, conf);
      try (ParquetReader<Group> reader = new GroupParquetReaderBuilder(inputFile).withConf(conf).build();
          ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile).withConf(conf).build()) {
        Group group;
        while ((group = reader.read()) != null) {
          writer.write(group);
          sourceRecordSignatures.add(group.toString());
          recordsCopied++;
          if (recordsCopied >= 100) break;
        }
      }
    }
    // Read the writtten content for verifying the correctness of the data written.
    InputFile destInputFile = new TestInputStreamInputFile(destCtx.uri, false, readFsOptions);
    List<String> destRecordSignatures = new ArrayList<>();
    try (ParquetReader<Group> reader = new GroupParquetReaderBuilder(destInputFile).withConf(conf).build()) {
      Group group;
      while ((group = reader.read()) != null) {
        destRecordSignatures.add(group.toString());
      }
    }

    assertThat(recordsCopied).isGreaterThan(0);
    assertThat(IntegrationTestHelper.objectPresentInBucket(destCtx.fileName)).isTrue();
    assertThat(destRecordSignatures).isEqualTo(sourceRecordSignatures);
  }

  @Test
  void write_multipleChunks_tracksPositionAccurately() throws IOException {
    TestFileContext ctx = createTestFileContext(FILE_PREFIX_TXT, SUFFIX_TXT);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    byte[] chunk1 = CHUNK1_CONTENT;
    byte[] chunk2 = CHUNK2_CONTENT;
    long initialPosition;
    long positionAfterChunk1;
    long positionAfterChunk2;

    try (GoogleCloudStorageOutputStream outputStream =
        GoogleCloudStorageOutputStream.create(createFileSystemWithWriteOptions(writeOptions), ctx.itemId)) {
      initialPosition = outputStream.getBytesWritten();
      outputStream.write(chunk1);
      positionAfterChunk1 = outputStream.getBytesWritten();
      outputStream.write(chunk2);
      positionAfterChunk2 = outputStream.getBytesWritten();
    }

    assertThat(initialPosition).isEqualTo(0L);
    assertThat(positionAfterChunk1).isEqualTo((long) chunk1.length);
    assertThat(positionAfterChunk2).isEqualTo((long) (chunk1.length + chunk2.length));
    assertThat(IntegrationTestHelper.objectPresentInBucket(ctx.fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(ctx.uri).getItemInfo().getSize())
        .isEqualTo((long) (chunk1.length + chunk2.length));
  }

  @Test
  void write_withGenerationMatchAndConflict_throwsStorageException() throws IOException {
    TestFileContext ctx = createTestFileContext(FILE_PREFIX_TXT, SUFFIX_TXT);
    GcsWriteOptions defaultOptions = GcsWriteOptions.builder().build();

    try (GoogleCloudStorageOutputStream outputStream =
        GoogleCloudStorageOutputStream.create(createFileSystemWithWriteOptions(defaultOptions), ctx.itemId)) {
      outputStream.write(ORIGINAL_CONTENT);
    }

    GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(ctx.uri);
    long currentGeneration = fileInfo.getItemInfo().getContentGeneration().orElse(0L);
    try (GoogleCloudStorageOutputStream outputStream =
        GoogleCloudStorageOutputStream.create(createFileSystemWithWriteOptions(defaultOptions), ctx.itemId)) {
      outputStream.write(CONCURRENT_CONTENT);
    }

    GcsItemId itemIdWithOldGeneration = GcsItemId.builder()
        .setBucketName(ctx.itemId.getBucketName())
        .setObjectName(ctx.itemId.getObjectName().get())
        .setContentGeneration(currentGeneration)
        .build();
    // Write with the stale generation should throw exception.
    IOException exception = assertThrows(IOException.class, () -> {
      try (GoogleCloudStorageOutputStream outputStream =
          GoogleCloudStorageOutputStream.create(gcsFileSystem, itemIdWithOldGeneration)) {
        outputStream.write(STALE_CONTENT);
      }
    });
    assertThat(currentGeneration).isGreaterThan(0L);
    assertThat(exception).hasMessageThat().contains(GENERATION_MISMATCH_MESSAGE);
  }

  @Test
  void write_withCustomerSuppliedEncryptionKey_createsFileSuccessfully() throws IOException {
    TestFileContext ctx = createTestFileContext(FILE_PREFIX_TXT, SUFFIX_TXT);
    GcsWriteOptions writeOptions = GcsWriteOptions.builder()
        .setEncryptionKey(CSEK_KEY)
        .build();
    GcsReadOptions readOptionsNoKey = GcsReadOptions.builder().build();
    GcsReadOptions readOptionsWithKey = GcsReadOptions.builder()
        .setDecryptionKey(CSEK_KEY)
        .build();
    int bytesRead;

    // Write encrypted data with the encryption key.
    try (GoogleCloudStorageOutputStream outputStream =
        GoogleCloudStorageOutputStream.create(createFileSystemWithWriteOptions(writeOptions), ctx.itemId)) {
      outputStream.write(CSEK_ENCRYPTED_CONTENT);
    }

    // Attempting to read without the encryption key should result in an exception.
    IOException readWithoutKeyException = assertThrows(IOException.class, () -> {
      try (VectoredSeekableByteChannel readChannel = gcsFileSystem.open(ctx.itemId, readOptionsNoKey)) {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        readChannel.read(buffer);
      }
    });

    // Attempting to read the data without the encryption key should result in an exception.
    ByteBuffer buffer = ByteBuffer.allocate(30);
    try (VectoredSeekableByteChannel readChannel = gcsFileSystem.open(ctx.itemId, readOptionsWithKey)) {
      bytesRead = readChannel.read(buffer);
      buffer.flip();
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(ctx.fileName)).isTrue();
    assertThat(readWithoutKeyException).isNotNull();
    assertThat(bytesRead).isGreaterThan(0);
    assertThat(new String(buffer.array(), 0, bytesRead, StandardCharsets.UTF_8))
        .isEqualTo(CSEK_RAW_SECRET);
  }
  
  private GcsFileSystem createFileSystemWithWriteOptions(GcsWriteOptions writeOptions) throws IOException {
    return createFileSystemWithClientOptions(GcsClientOptions.builder().setGcsWriteOptions(writeOptions).build());
  }

  private GcsFileSystem createFileSystemWithClientOptions(GcsClientOptions clientOptions) throws IOException {
    GcsFileSystemOptions options = GcsFileSystemOptions.builder()
        .setGcsClientOptions(clientOptions)
        .build();
    return new GcsFileSystemImpl(options);
  }
}
