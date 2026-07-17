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
package com.google.cloud.gcs.analyticscore.client;

import com.google.cloud.ReadChannel;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to extract metadata from GCS SDK {@link ReadChannel} instances using reflection.
 */
final class GcsReadChannelMetadataExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(GcsReadChannelMetadataExtractor.class);

  private static final ImmutableList<String> METADATA_METHOD_NAMES =
      ImmutableList.of(
          "getObject", "getResolvedObject", "getBlobInfo", "getBlob", "getStorageObject");

  private static final ImmutableList<String> METADATA_FIELD_NAMES =
      ImmutableList.of("storageObject", "blobInfo", "object", "result");

  private GcsReadChannelMetadataExtractor() {}

  static final class ExtractedMetadata {
    private final long size;
    private final long generation;

    ExtractedMetadata(long size, long generation) {
      this.size = size;
      this.generation = generation;
    }

    long getSize() {
      return size;
    }

    long getGeneration() {
      return generation;
    }
  }

  @Nullable
  static ExtractedMetadata extract(@Nullable ReadChannel sdkChannel) {
    if (sdkChannel == null) {
      return null;
    }
    Object resolvedMetadata = resolveMetadataObject(sdkChannel);
    if (resolvedMetadata == null) {
      return null;
    }
    long extractedSize = extractLongProperty(resolvedMetadata, "getSize", "size");
    if (extractedSize < 0) {
      return null;
    }
    long extractedGeneration = extractLongProperty(resolvedMetadata, "getGeneration", "generation");
    return new ExtractedMetadata(extractedSize, extractedGeneration);
  }

  @Nullable
  private static Object resolveMetadataObject(ReadChannel sdkChannel) {
    Class<?> clazz = sdkChannel.getClass();
    while (clazz != null) {
      for (String methodName : METADATA_METHOD_NAMES) {
        try {
          Method method = clazz.getDeclaredMethod(methodName);
          method.setAccessible(true);
          Object resolvedMetadata = resolveFutureIfNeeded(method.invoke(sdkChannel));
          if (resolvedMetadata != null) {
            return resolvedMetadata;
          }
        } catch (ReflectiveOperationException | RuntimeException ignored) {
          LOG.debug(
              "Method {} not present or inaccessible on class {}", methodName, clazz.getName());
        }
      }
      for (String fieldName : METADATA_FIELD_NAMES) {
        try {
          Field field = clazz.getDeclaredField(fieldName);
          field.setAccessible(true);
          Object resolvedMetadata = resolveFutureIfNeeded(field.get(sdkChannel));
          if (resolvedMetadata != null) {
            return resolvedMetadata;
          }
        } catch (ReflectiveOperationException | RuntimeException ignored) {
          LOG.debug("Field {} not present or inaccessible on class {}", fieldName, clazz.getName());
        }
      }
      clazz = clazz.getSuperclass();
    }
    return null;
  }

  @Nullable
  private static Object resolveFutureIfNeeded(@Nullable Object obj)
      throws ReflectiveOperationException {
    if (!(obj instanceof Future)) {
      return obj;
    }
    Future<?> future = (Future<?>) obj;
    if (!future.isDone()) {
      return null;
    }
    try {
      return future.get();
    } catch (CancellationException | ExecutionException ignored) {
      // If the future failed or was cancelled, it is safe to fallback to null and attempt
      // field-based metadata extraction instead.
      LOG.debug("Future execution failed or was cancelled", ignored);
      return null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  private static long extractLongProperty(
      Object target, String primaryGetter, String fallbackGetter) {
    long value =
        Arrays.stream(target.getClass().getMethods())
            .filter(m -> m.getParameterCount() == 0 && m.getName().equals(primaryGetter))
            .mapToLong(m -> invokeLongGetter(target, m))
            .filter(v -> v >= 0)
            .findFirst()
            .orElse(-1L);
    if (value >= 0) {
      return value;
    }
    if (target instanceof Map || target instanceof Collection) {
      return -1L;
    }
    return Arrays.stream(target.getClass().getMethods())
        .filter(m -> m.getParameterCount() == 0 && m.getName().equals(fallbackGetter))
        .mapToLong(m -> invokeLongGetter(target, m))
        .filter(v -> v >= 0)
        .findFirst()
        .orElse(-1L);
  }

  private static long invokeLongGetter(Object target, Method method) {
    try {
      method.setAccessible(true);
      Object value = method.invoke(target);
      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
    } catch (ReflectiveOperationException | RuntimeException ignored) {
      LOG.debug(
          "Getter invocation failed or inaccessible for method {} on target {}",
          method.getName(),
          target);
    }
    return -1L;
  }
}
