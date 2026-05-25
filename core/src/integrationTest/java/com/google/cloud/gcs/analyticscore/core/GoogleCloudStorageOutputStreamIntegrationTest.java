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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(named = IntegrationTestHelper.GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
@EnabledIfSystemProperty(named = IntegrationTestHelper.GCS_INTEGRATION_TEST_PROJECT_ID_PROPERTY, matches = ".+")
class GoogleCloudStorageOutputStreamIntegrationTest {

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
  private static final String NON_EXISTENT_BUCKET = "non-existent-bucket-random-name-12345";
  private static final String NON_EXISTENT_FILE = "test-file.txt";

  private GcsFileSystem gcsFileSystem;
  private List<BlobId> blobsToDelete;

  @BeforeEach
  void setUp() {
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
          // Ignore cleanup errors
        }
      }
    }
  }

  private String getRandomFileName(String prefix, String suffix) {
    return prefix + UUID.randomUUID() + suffix;
  }

  @Test
  void writeNormalBytes_success() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_TXT, SUFFIX_TXT);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, writeOptions)) {
      outputStream.write(TEST_CONTENT);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(uri).getItemInfo().getSize()).isEqualTo((long) TEST_CONTENT.length);
  }

  @Test
  void writeCsvFile_success() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_CSV, SUFFIX_CSV);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, writeOptions)) {
      outputStream.write(CSV_CONTENT);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(uri).getItemInfo().getSize()).isEqualTo((long) CSV_CONTENT.length);
  }

  @Test
  void writeEmptyFile_success() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_TXT, SUFFIX_TXT);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, writeOptions)) {
      // Write nothing, just open and close
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(uri).getItemInfo().getSize()).isEqualTo(0L);
  }

  @Test
  void overwriteDisabled_throwsException() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_TXT, SUFFIX_TXT);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

    GcsWriteOptions defaultOptions = GcsWriteOptions.builder().build();
    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, defaultOptions)) {
      outputStream.write(FIRST_WRITE_CONTENT);
    }

    GcsWriteOptions noOverwriteOptions = GcsWriteOptions.builder()
        .setOverwriteExisting(false)
        .build();

    assertThrows(FileAlreadyExistsException.class, () -> {
      try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, noOverwriteOptions)) {
        outputStream.write(SECOND_WRITE_CONTENT);
      }
    });
  }

  @Test
  void overwriteEnabled_overwritesExistingFile() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_TXT, SUFFIX_TXT);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

    GcsWriteOptions defaultOptions = GcsWriteOptions.builder().build();
    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, defaultOptions)) {
      outputStream.write(FIRST_WRITE_CONTENT);
    }

    GcsWriteOptions overwriteOptions = GcsWriteOptions.builder()
        .setOverwriteExisting(true)
        .build();

    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, overwriteOptions)) {
      outputStream.write(SECOND_WRITE_CONTENT);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(uri).getItemInfo().getSize()).isEqualTo((long) SECOND_WRITE_CONTENT.length);
  }

  @Test
  void writeWithChecksumValidation_success() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_TXT, SUFFIX_TXT);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

    GcsWriteOptions writeOptions = GcsWriteOptions.builder()
        .setChecksumValidationEnabled(true)
        .build();

    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, writeOptions)) {
      outputStream.write(TEST_CONTENT);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(uri).getItemInfo().getSize()).isEqualTo((long) TEST_CONTENT.length);
  }

  @Test
  void writeLargeFile_multipleChunks_success() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_BIN, SUFFIX_BIN);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

    GcsWriteOptions writeOptions = GcsWriteOptions.builder()
        .setUploadChunkSize(256 * 1024)
        .build();

    int totalSize = 1024 * 1024; // 1 MB total size
    byte[] chunk = new byte[1024]; // 1 KB chunks written locally

    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, writeOptions)) {
      for (int i = 0; i < totalSize / chunk.length; i++) {
        outputStream.write(chunk);
      }
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(uri).getItemInfo().getSize()).isEqualTo((long) totalSize);
  }

  @Test
  void writeWithParallelCompositeUpload_success() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_TXT, SUFFIX_TXT);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

    GcsWriteOptions writeOptions = GcsWriteOptions.builder()
        .setUploadType(GcsWriteOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
        .setPcuBufferCount(2)
        .setPcuBufferCapacity(16 * 1024 * 1024) // 16MB
        .setPcuPartFileCleanupType(GcsWriteOptions.PartFileCleanupType.ALWAYS)
        .setPcuPartFileNamePrefix("pcu-part-")
        .build();

    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, writeOptions)) {
      outputStream.write(TEST_CONTENT);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(uri).getItemInfo().getSize()).isEqualTo((long) TEST_CONTENT.length);
  }

  @Test
  void writeToDiskThenUpload_success() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_TXT, SUFFIX_TXT);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

    GcsWriteOptions writeOptions = GcsWriteOptions.builder()
        .setUploadType(GcsWriteOptions.UploadType.WRITE_TO_DISK_THEN_UPLOAD)
        .build();

    try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, writeOptions)) {
      outputStream.write(TEST_CONTENT);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(uri).getItemInfo().getSize()).isEqualTo((long) TEST_CONTENT.length);
  }

  @Test
  void writeWithJournaling_throwsUnsupportedOperationException() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_TXT, SUFFIX_TXT);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

    Path tempDir = Files.createTempDirectory("gcs-journal-test");
    try {
      GcsWriteOptions writeOptions = GcsWriteOptions.builder()
          .setUploadType(GcsWriteOptions.UploadType.JOURNALING)
          .setTemporaryPaths(Collections.singletonList(tempDir.toAbsolutePath().toString()))
          .build();

      assertThrows(UnsupportedOperationException.class, () -> {
        try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, writeOptions)) {
          outputStream.write(TEST_CONTENT);
        }
      });
    } finally {
      Files.deleteIfExists(tempDir);
    }
  }

  @Test
  void writeToNonExistentBucket_throwsFileNotFoundException() {
    BlobId blobId = BlobId.of(NON_EXISTENT_BUCKET, NON_EXISTENT_FILE);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

    assertThrows(FileNotFoundException.class, () -> {
      try (GoogleCloudStorageOutputStream outputStream = GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, writeOptions)) {
        outputStream.write(TEST_CONTENT);
      }
    });
  }

  @Test
  void writeParquetFile_success() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_PARQUET, SUFFIX_PARQUET);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    WritableByteChannel writeChannel = gcsFileSystem.create(blobInfo, writeOptions);
    OutputFile outputFile = new TestOutputStreamOutputFile(writeChannel);
    MessageType schema = MessageTypeParser.parseMessageType(PARQUET_SCHEMA_STRING);
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
        .withConf(conf)
        .build()) {
      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
      writer.write(groupFactory.newGroup().append(PARQUET_FIELD_NAME, PARQUET_VAL_ALICE));
      writer.write(groupFactory.newGroup().append(PARQUET_FIELD_NAME, PARQUET_VAL_BOB));
    }
    
    // Read the writtten content for verifying the correctness of the data written.
    GcsFileSystemOptions readFsOptions = GcsFileSystemOptions.createFromOptions(Map.of(), "gcs.");
    InputFile inputFile = new TestInputStreamInputFile(uri, false, readFsOptions);
    List<String> namesRead = new ArrayList<>();
    try (ParquetReader<Group> reader = new GroupParquetReaderBuilder(inputFile).withConf(conf).build()) {
      Group group;
      while ((group = reader.read()) != null) {
        namesRead.add(group.getString(PARQUET_FIELD_NAME, 0));
      }
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(namesRead).containsExactly(PARQUET_VAL_ALICE, PARQUET_VAL_BOB).inOrder();
  }

  @Test
  void imitateCtasQuery_readAndWriteParquet() throws IOException {
    IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
    URI sourceUri = IntegrationTestHelper.getGcsObjectUriForFile(IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE);
    GcsFileSystemOptions readFsOptions = GcsFileSystemOptions.createFromOptions(Map.of(), "gcs.");
    InputFile inputFile = new TestInputStreamInputFile(sourceUri, false, readFsOptions);
    ParquetMetadata metadata =
        ParquetHelper.readParquetMetadata(sourceUri, readFsOptions);
    MessageType schema = metadata.getFileMetaData().getSchema();
    String destFileName = getRandomFileName(FILE_PREFIX_CTAS, SUFFIX_PARQUET);
    URI destUri = IntegrationTestHelper.getGcsObjectUriForFile(destFileName);
    BlobId destBlobId = BlobId.fromGsUtilUri(destUri.toString());
    blobsToDelete.add(destBlobId);
    BlobInfo destBlobInfo = BlobInfo.newBuilder(destBlobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    WritableByteChannel writeChannel = gcsFileSystem.create(destBlobInfo, writeOptions);
    OutputFile outputFile = new TestOutputStreamOutputFile(writeChannel);
    int recordsCopied = 0;
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);
    List<String> sourceRecordSignatures = new ArrayList<>();

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
    // Read the writtten content for verifying the correctness of the data written.
    InputFile destInputFile = new TestInputStreamInputFile(destUri, false, readFsOptions);
    List<String> destRecordSignatures = new ArrayList<>();
    try (ParquetReader<Group> reader = new GroupParquetReaderBuilder(destInputFile).withConf(conf).build()) {
      Group group;
      while ((group = reader.read()) != null) {
        destRecordSignatures.add(group.toString());
      }
    }

    assertThat(recordsCopied).isGreaterThan(0);
    assertThat(IntegrationTestHelper.objectPresentInBucket(destFileName)).isTrue();
    assertThat(destRecordSignatures).isEqualTo(sourceRecordSignatures);
  }

  @Test
  void writeBytes_tracksPositionAccurately() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_TXT, SUFFIX_TXT);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    byte[] chunk1 = CHUNK1_CONTENT;
    byte[] chunk2 = CHUNK2_CONTENT;
    long initialPosition;
    long positionAfterChunk1;
    long positionAfterChunk2;

    try (GoogleCloudStorageOutputStream outputStream =
        GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, writeOptions)) {
      initialPosition = outputStream.getBytesWritten();
      outputStream.write(chunk1);
      positionAfterChunk1 = outputStream.getBytesWritten();
      outputStream.write(chunk2);
      positionAfterChunk2 = outputStream.getBytesWritten();
    }

    assertThat(initialPosition).isEqualTo(0L);
    assertThat(positionAfterChunk1).isEqualTo((long) chunk1.length);
    assertThat(positionAfterChunk2).isEqualTo((long) (chunk1.length + chunk2.length));
    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(uri).getItemInfo().getSize())
        .isEqualTo((long) (chunk1.length + chunk2.length));
  }

  @Test
  void writeWithGenerationMatch_conflictThrowsIOException() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_TXT, SUFFIX_TXT);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions defaultOptions = GcsWriteOptions.builder().build();

    try (GoogleCloudStorageOutputStream outputStream =
        GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, defaultOptions)) {
      outputStream.write(ORIGINAL_CONTENT);
    }

    GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(uri);
    long currentGeneration = fileInfo.getItemInfo().getContentGeneration().orElse(0L);
    try (GoogleCloudStorageOutputStream outputStream =
        GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, defaultOptions)) {
      outputStream.write(CONCURRENT_CONTENT);
    }

    BlobInfo blobInfoWithOldGeneration = BlobInfo.newBuilder(
        BlobId.of(blobId.getBucket(), blobId.getName(), currentGeneration)).build();
    IOException exception = assertThrows(IOException.class, () -> {
      try (GoogleCloudStorageOutputStream outputStream =
          GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfoWithOldGeneration, defaultOptions)) {
        outputStream.write(STALE_CONTENT);
      }
    });

    assertThat(currentGeneration).isGreaterThan(0L);
    assertThat(exception).hasMessageThat().contains(GENERATION_MISMATCH_MESSAGE);
  }

  @Test
  void writeWithCustomerSuppliedEncryptionKey_success() throws IOException {
    String fileName = getRandomFileName(FILE_PREFIX_TXT, SUFFIX_TXT);
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    blobsToDelete.add(blobId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder()
        .setEncryptionKey(CSEK_KEY)
        .build();
    GcsItemId itemId = GcsItemId.builder()
        .setBucketName(blobId.getBucket())
        .setObjectName(blobId.getName())
        .build();
    GcsReadOptions readOptionsNoKey = GcsReadOptions.builder().build();
    GcsReadOptions readOptionsWithKey = GcsReadOptions.builder()
        .setDecryptionKey(CSEK_KEY)
        .build();

    try (GoogleCloudStorageOutputStream outputStream =
        GoogleCloudStorageOutputStream.create(gcsFileSystem, blobInfo, writeOptions)) {
      outputStream.write(CSEK_ENCRYPTED_CONTENT);
    }
    IOException readWithoutKeyException = assertThrows(IOException.class, () -> {
      try (VectoredSeekableByteChannel readChannel = gcsFileSystem.open(itemId, readOptionsNoKey)) {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        readChannel.read(buffer);
      }
    });
    int bytesRead;
    ByteBuffer buffer = ByteBuffer.allocate(30);
    try (VectoredSeekableByteChannel readChannel = gcsFileSystem.open(itemId, readOptionsWithKey)) {
      bytesRead = readChannel.read(buffer);
      buffer.flip();
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(readWithoutKeyException).isNotNull();
    assertThat(bytesRead).isGreaterThan(0);
    assertThat(new String(buffer.array(), 0, bytesRead, StandardCharsets.UTF_8))
        .isEqualTo(CSEK_RAW_SECRET);
  }
}
