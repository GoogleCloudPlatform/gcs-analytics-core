package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class VersionHelperTest {

  @Test
  void loadVersion_validPomStream_returnsVersion() {
    String pomContent = "version=1.2.3-TEST\n";
    InputStream inputStream = new ByteArrayInputStream(pomContent.getBytes(StandardCharsets.UTF_8));
    String version = VersionHelper.loadVersion(inputStream);
    assertThat(version).isEqualTo("1.2.3-TEST");
  }

  @Test
  void loadVersion_nullStream_returnsDefault() {
    String version = VersionHelper.loadVersion((InputStream) null);
    assertThat(version).isEqualTo(VersionHelper.DEFAULT_VERSION);
  }
}
