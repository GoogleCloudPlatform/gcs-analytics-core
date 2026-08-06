package com.google.cloud.gcs.analyticscore.client;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/** GCS provided validation attributes for a single object. */
@AutoValue
public abstract class VerificationAttributes {
  @Nullable
  @SuppressWarnings("mutable")
  public abstract byte[] getMd5hash();

  @Nullable
  @SuppressWarnings("mutable")
  public abstract byte[] getCrc32c();

  public static VerificationAttributes create(@Nullable byte[] md5hash, @Nullable byte[] crc32c) {
    return new AutoValue_VerificationAttributes(md5hash, crc32c);
  }
}
