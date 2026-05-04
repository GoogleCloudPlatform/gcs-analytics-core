package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.FileAlreadyExistsException;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

class GoogleCloudStorageWriteChannelIntegrationTest {

  private GcsFileSystem gcsFileSystem;

  @BeforeEach
  void setUp() {
    GcsFileSystemOptions options = GcsFileSystemOptions.builder()
        .setGcsClientOptions(GcsClientOptions.builder().build())
        .build();
    gcsFileSystem = new GcsFileSystemImpl(options);
  }

  @Test
  void writeNormalBytes_success() throws IOException {
    String fileName = "test-bytes-write.txt";
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

    try (GoogleCloudStorageWriteChannel channel = new GoogleCloudStorageWriteChannel(gcsFileSystem, blobInfo, writeOptions)) {
      ByteBuffer buffer = ByteBuffer.wrap("Writing normal bytes!".getBytes(StandardCharsets.UTF_8));
      channel.write(buffer);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
  }

  @Test
  void writeCsvFile_success() throws IOException {
    String fileName = "test-data.csv";
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    String csvContent = "id,name,city\n1,Alice,NYC\n2,Bob,SFO\n";

    try (GoogleCloudStorageWriteChannel channel = new GoogleCloudStorageWriteChannel(gcsFileSystem, blobInfo, writeOptions)) {
      ByteBuffer buffer = ByteBuffer.wrap(csvContent.getBytes(StandardCharsets.UTF_8));
      channel.write(buffer);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
  }

  @Test
  void writeParquetFile_success() throws IOException {
    String fileName = "test-parquet-write.parquet";
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    GoogleCloudStorageWriteChannel writeChannel = new GoogleCloudStorageWriteChannel(gcsFileSystem, blobInfo, writeOptions);
    OutputFile outputFile = new TestOutputStreamOutputFile(writeChannel);
    MessageType schema = MessageTypeParser.parseMessageType("message test { required binary name (UTF8); }");
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
        .withConf(conf)
        .build()) {
      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
      writer.write(groupFactory.newGroup().append("name", "Alice"));
      writer.write(groupFactory.newGroup().append("name", "Bob"));
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
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
    String destFileName = "ctas_output.parquet";
    URI destUri = IntegrationTestHelper.getGcsObjectUriForFile(destFileName);
    BlobId destBlobId = BlobId.fromGsUtilUri(destUri.toString());
    BlobInfo destBlobInfo = BlobInfo.newBuilder(destBlobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
    GoogleCloudStorageWriteChannel writeChannel = new GoogleCloudStorageWriteChannel(gcsFileSystem, destBlobInfo, writeOptions);
    OutputFile outputFile = new TestOutputStreamOutputFile(writeChannel);
    int recordsCopied = 0;
    Configuration conf = new Configuration();
    // Feed the extracted schema to the configuration so the writer knows what to do
    GroupWriteSupport.setSchema(schema, conf);

    try (ParquetReader<Group> reader = new GroupParquetReaderBuilder(inputFile).withConf(conf).build();
        ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile).withConf(conf).build()) {
      Group group;
      while ((group = reader.read()) != null) {
        writer.write(group);
        recordsCopied++;
        if (recordsCopied >= 100) break;
      }
    }

    assertThat(recordsCopied).isGreaterThan(0);
    assertThat(IntegrationTestHelper.objectPresentInBucket(destFileName)).isTrue();
  }

  @Test
  void writeEmptyFile_success() throws IOException {
    String fileName = "test-empty-file.txt";
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

    try (GoogleCloudStorageWriteChannel channel = new GoogleCloudStorageWriteChannel(gcsFileSystem, blobInfo, writeOptions)) {
      // Write absolutely nothing, just open and close
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(uri).getItemInfo().getSize()).isEqualTo(0L);
  }

  @Test
  void overwriteDisabled_throwsException() throws IOException {
    String fileName = "test-overwrite-disabled.txt";
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

    // 1. First write (should succeed)
    GcsWriteOptions defaultOptions = GcsWriteOptions.builder().build();
    try (GoogleCloudStorageWriteChannel channel = new GoogleCloudStorageWriteChannel(gcsFileSystem, blobInfo, defaultOptions)) {
      channel.write(ByteBuffer.wrap("First write".getBytes(StandardCharsets.UTF_8)));
    }

    // 2. Second write with overwrite=false (should fail)
    GcsWriteOptions noOverwriteOptions = GcsWriteOptions.builder()
        .setOverwriteExisting(false)
        .build();

    assertThrows(FileAlreadyExistsException.class, () -> {
      try (GoogleCloudStorageWriteChannel channel = new GoogleCloudStorageWriteChannel(gcsFileSystem, blobInfo, noOverwriteOptions)) {
        channel.write(ByteBuffer.wrap("Second write attempts to overwrite".getBytes(StandardCharsets.UTF_8)));
      }
    });
  }

  @Test
  void writeWithChecksumValidation_success() throws IOException {
    String fileName = "test-checksum-validation.txt";
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

    // Enable CRC32C Checksum validation
    GcsWriteOptions writeOptions = GcsWriteOptions.builder()
        .setChecksumValidationEnabled(true)
        .build();

    try (GoogleCloudStorageWriteChannel channel = new GoogleCloudStorageWriteChannel(gcsFileSystem, blobInfo, writeOptions)) {
      ByteBuffer buffer = ByteBuffer.wrap("Checksum validated content!".getBytes(StandardCharsets.UTF_8));
      channel.write(buffer);
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
  }

  @Test
  void writeLargeFile_multipleChunks_success() throws IOException {
    String fileName = "test-large-chunked-file.bin";
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    BlobId blobId = BlobId.fromGsUtilUri(uri.toString());
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

    // Set a small chunk size (256 KB) to force multiple HTTP requests
    GcsWriteOptions writeOptions = GcsWriteOptions.builder()
        .setUploadChunkSize(256 * 1024)
        .build();

    int totalSize = 1024 * 1024; // 1 MB total size
    byte[] chunk = new byte[1024]; // 1 KB chunks written locally

    try (GoogleCloudStorageWriteChannel channel = new GoogleCloudStorageWriteChannel(gcsFileSystem, blobInfo, writeOptions)) {
      for (int i = 0; i < totalSize / chunk.length; i++) {
        ByteBuffer buffer = ByteBuffer.wrap(chunk);
        channel.write(buffer);
      }
    }

    assertThat(IntegrationTestHelper.objectPresentInBucket(fileName)).isTrue();
    assertThat(gcsFileSystem.getFileInfo(uri).getItemInfo().getSize()).isEqualTo((long) totalSize);
  }
}
