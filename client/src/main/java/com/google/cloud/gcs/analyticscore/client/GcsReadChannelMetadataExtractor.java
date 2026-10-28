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
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
          "getResolvedObject", "getObject", "getBlobInfo", "getBlob", "getStorageObject");

  private static final ImmutableList<String> METADATA_FIELD_NAMES =
      ImmutableList.of("blobInfo", "storageObject", "object", "result");

  private static final String SDK_CHANNEL_DECORATOR_SIMPLE_NAME = "OtelDecoratedReadChannel";
  private static final String SDK_CHANNEL_DECORATOR_FIELD_NAME = "reader";

  private static final ClassValue<ChannelAccessors> CHANNEL_ACCESSORS =
      new ClassValue<ChannelAccessors>() {
        @Override
        protected ChannelAccessors computeValue(Class<?> type) {
          return ChannelAccessors.forClass(type);
        }
      };

  private static final ClassValue<PropertyAccessors> PROPERTY_ACCESSORS =
      new ClassValue<PropertyAccessors>() {
        @Override
        protected PropertyAccessors computeValue(Class<?> type) {
          return PropertyAccessors.forClass(type);
        }
      };

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

  private static final class ChannelAccessors {
    @Nullable final Field decoratorReaderField;
    final ImmutableList<AccessibleObject> metadataAccessors;

    private ChannelAccessors(
        @Nullable Field decoratorReaderField, ImmutableList<AccessibleObject> metadataAccessors) {
      this.decoratorReaderField = decoratorReaderField;
      this.metadataAccessors = metadataAccessors;
    }

    static ChannelAccessors forClass(Class<?> type) {
      Field decoratorField = null;
      if (SDK_CHANNEL_DECORATOR_SIMPLE_NAME.equals(type.getSimpleName())) {
        try {
          Field field = type.getDeclaredField(SDK_CHANNEL_DECORATOR_FIELD_NAME);
          field.setAccessible(true);
          decoratorField = field;
        } catch (ReflectiveOperationException | RuntimeException e) {
          LOG.debug(
              "Failed reading field {} on {}", SDK_CHANNEL_DECORATOR_FIELD_NAME, type.getName(), e);
        }
      }

      ImmutableList.Builder<AccessibleObject> accessors = ImmutableList.builder();
      for (Class<?> clazz = type; clazz != Object.class; clazz = clazz.getSuperclass()) {
        for (String methodName : METADATA_METHOD_NAMES) {
          try {
            Method method = clazz.getDeclaredMethod(methodName);
            method.setAccessible(true);
            accessors.add(method);
          } catch (NoSuchMethodException | RuntimeException ignored) {
            // Expected: the candidate accessor does not exist on this class; try the next one.
          }
        }
        for (String fieldName : METADATA_FIELD_NAMES) {
          try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            accessors.add(field);
          } catch (NoSuchFieldException | RuntimeException ignored) {
            // Expected: the candidate field does not exist on this class; try the next one.
          }
        }
      }
      return new ChannelAccessors(decoratorField, accessors.build());
    }
  }

  private static final class PropertyAccessors {
    final ImmutableList<Method> sizeGetters;
    final ImmutableList<Method> generationGetters;

    private PropertyAccessors(
        ImmutableList<Method> sizeGetters, ImmutableList<Method> generationGetters) {
      this.sizeGetters = sizeGetters;
      this.generationGetters = generationGetters;
    }

    static PropertyAccessors forClass(Class<?> type) {
      Method[] methods = type.getMethods();
      boolean allowFallback =
          !Map.class.isAssignableFrom(type) && !Collection.class.isAssignableFrom(type);
      return new PropertyAccessors(
          findZeroArgGetters(methods, "getSize", allowFallback ? "size" : null),
          findZeroArgGetters(methods, "getGeneration", allowFallback ? "generation" : null));
    }

    private static ImmutableList<Method> findZeroArgGetters(
        Method[] methods, String primaryGetter, @Nullable String fallbackGetter) {
      ImmutableList.Builder<Method> getters = ImmutableList.builder();
      addMatchingZeroArgMethods(methods, primaryGetter, getters);
      if (fallbackGetter != null) {
        addMatchingZeroArgMethods(methods, fallbackGetter, getters);
      }
      return getters.build();
    }

    private static void addMatchingZeroArgMethods(
        Method[] methods, String name, ImmutableList.Builder<Method> getters) {
      for (Method method : methods) {
        if (method.getParameterCount() == 0 && method.getName().equals(name)) {
          try {
            method.setAccessible(true);
            getters.add(method);
          } catch (RuntimeException e) {
            LOG.debug("Failed accessing method {} on {}", name, method.getDeclaringClass(), e);
          }
        }
      }
    }
  }

  @Nullable
  static ExtractedMetadata extract(@Nullable ReadChannel sdkChannel) {
    if (sdkChannel == null) {
      return null;
    }
    ChannelAccessors channelAccessors = CHANNEL_ACCESSORS.get(sdkChannel.getClass());
    Object target = sdkChannel;
    if (channelAccessors.decoratorReaderField != null) {
      Object delegate = invokeAccessor(sdkChannel, channelAccessors.decoratorReaderField);
      if (delegate != null) {
        target = delegate;
        channelAccessors = CHANNEL_ACCESSORS.get(target.getClass());
      }
    }
    return extractFromTarget(target, channelAccessors);
  }

  @Nullable
  private static ExtractedMetadata extractFromTarget(
      Object target, ChannelAccessors channelAccessors) {
    ImmutableList<AccessibleObject> accessors = channelAccessors.metadataAccessors;
    for (int i = 0; i < accessors.size(); i++) {
      Object resolvedMetadata = resolveFutureIfNeeded(invokeAccessor(target, accessors.get(i)));
      if (resolvedMetadata == null) {
        continue;
      }
      PropertyAccessors propertyAccessors = PROPERTY_ACCESSORS.get(resolvedMetadata.getClass());
      long extractedSize = extractLongProperty(resolvedMetadata, propertyAccessors.sizeGetters);
      if (extractedSize >= 0) {
        long extractedGeneration =
            extractLongProperty(resolvedMetadata, propertyAccessors.generationGetters);
        return new ExtractedMetadata(extractedSize, extractedGeneration);
      }
    }
    return null;
  }

  @Nullable
  private static Object invokeAccessor(Object target, AccessibleObject accessor) {
    try {
      return accessor instanceof Method
          ? ((Method) accessor).invoke(target)
          : ((Field) accessor).get(target);
    } catch (ReflectiveOperationException | RuntimeException e) {
      LOG.debug(
          "Failed reading metadata accessor {} on {}", accessor, target.getClass().getName(), e);
      return null;
    }
  }

  @Nullable
  private static Object resolveFutureIfNeeded(@Nullable Object obj) {
    if (obj == null) {
      return null;
    }
    if (!(obj instanceof Future)) {
      return obj;
    }
    Future<?> future = (Future<?>) obj;
    if (!future.isDone()) {
      return null;
    }
    try {
      return future.get();
    } catch (CancellationException | ExecutionException e) {
      LOG.debug("Future execution failed or was cancelled", e);
      return null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  private static long extractLongProperty(Object target, ImmutableList<Method> getters) {
    for (int i = 0; i < getters.size(); i++) {
      long value = invokeLongGetter(target, getters.get(i));
      if (value >= 0) {
        return value;
      }
    }
    return -1L;
  }

  private static long invokeLongGetter(Object target, Method method) {
    try {
      Object value = method.invoke(target);
      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
    } catch (ReflectiveOperationException | RuntimeException e) {
      LOG.debug(
          "Getter invocation failed or inaccessible for method {} on target {}",
          method.getName(),
          target);
    }
    return -1L;
  }
}
