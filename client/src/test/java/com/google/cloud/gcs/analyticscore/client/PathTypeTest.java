package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;

class PathTypeTest {

  private static final String BUCKET_NAME = "test-bucket";

  @Test
  void resolve_root_returnsRoot() {
    GcsItemId itemId = GcsItemId.builder().setBucketName("").setObjectName("").build();

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.ROOT);
  }

  @Test
  void resolve_nullItemId_throwsNullPointerException() {
    org.junit.jupiter.api.Assertions.assertThrows(
        NullPointerException.class, () -> PathType.resolve(null));
  }

  @Test
  void resolve_bucket_returnsBucket() {
    GcsItemId itemId = GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("").build();

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.BUCKET);
  }

  @Test
  void resolve_directory_returnsDirectory() {
    GcsItemId itemId = GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("foo/").build();

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.DIRECTORY);
  }

  @Test
  void resolve_knownExtension_returnsFile() {
    for (String ext : new String[] {".parquet", ".csv", ".json", ".avro", ".orc", ".txt"}) {
      GcsItemId itemId =
          GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("foo" + ext).build();

      PathType result = PathType.resolve(itemId);

      assertThat(result).isEqualTo(PathType.FILE);
    }
  }

  @Test
  void resolve_nullBucketName_returnsRoot() {
    GcsItemId itemId = GcsItemId.ROOT;

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.ROOT);
  }

  @Test
  void resolve_unknownExtension_returnsUnknown() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("foo.bar").build();

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.UNKNOWN);
  }

  @Test
  void resolve_noExtension_returnsUnknown() {
    GcsItemId itemId = GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("foo").build();

    PathType result = PathType.resolve(itemId);

    assertThat(result).isEqualTo(PathType.UNKNOWN);
  }
}
