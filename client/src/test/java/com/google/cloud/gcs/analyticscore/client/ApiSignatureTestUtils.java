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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

/** Test assertions guarding public client classes against Guava signature leaks. */
final class ApiSignatureTestUtils {

  private static final String GUAVA_PACKAGE_PREFIX = "com.google.common.";

  private ApiSignatureTestUtils() {}

  /**
   * Verifies that none of the given classes declare supertypes, type-parameter bounds, fields,
   * constructors, or methods exposing Guava ({@code com.google.common.*}) types in their generic
   * signatures or {@code throws} clauses.
   */
  static void assertNoGuavaTypesInSignatures(Class<?>... classes) {
    checkNotNull(classes, "classes must not be null");
    checkArgument(classes.length > 0, "classes must not be empty");
    for (Class<?> clazz : classes) {
      checkNotNull(clazz, "class element must not be null");
      Type genericSuperclass = clazz.getGenericSuperclass();
      if (genericSuperclass != null) {
        checkNoGuavaType(clazz, "superclass", genericSuperclass);
      }
      checkNoGuavaTypes(clazz, "interfaces", clazz.getGenericInterfaces());
      checkNoGuavaTypeParameters(clazz, clazz.getName(), clazz.getTypeParameters());
      for (Field field : clazz.getDeclaredFields()) {
        checkNoGuavaType(clazz, field.toString(), field.getGenericType());
      }
      for (Constructor<?> ctor : clazz.getDeclaredConstructors()) {
        checkNoGuavaTypeParameters(clazz, ctor.toString(), ctor.getTypeParameters());
        checkNoGuavaTypes(clazz, ctor.toString(), ctor.getGenericParameterTypes());
        checkNoGuavaTypes(clazz, ctor.toString(), ctor.getGenericExceptionTypes());
      }
      for (Method method : clazz.getDeclaredMethods()) {
        checkNoGuavaTypeParameters(clazz, method.toString(), method.getTypeParameters());
        checkNoGuavaType(clazz, method.toString(), method.getGenericReturnType());
        checkNoGuavaTypes(clazz, method.toString(), method.getGenericParameterTypes());
        checkNoGuavaTypes(clazz, method.toString(), method.getGenericExceptionTypes());
      }
    }
  }

  private static void checkNoGuavaTypeParameters(
      Class<?> owner, String member, TypeVariable<?>[] typeParameters) {
    for (TypeVariable<?> typeParameter : typeParameters) {
      checkNoGuavaTypes(
          owner, member + " <" + typeParameter.getName() + ">", typeParameter.getBounds());
    }
  }

  private static void checkNoGuavaTypes(Class<?> owner, String member, Type[] types) {
    for (Type type : types) {
      checkNoGuavaType(owner, member, type);
    }
  }

  private static void checkNoGuavaType(Class<?> owner, String member, Type type) {
    if (type.getTypeName().contains(GUAVA_PACKAGE_PREFIX)) {
      throw new AssertionError(
          "Guava type " + type.getTypeName() + " leaked in " + member + " on " + owner.getName());
    }
  }
}
