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
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/** Test assertions guarding public client classes against Guava signature leaks. */
final class ApiSignatureTestUtils {

  private static final String GUAVA_PACKAGE_PREFIX = "com.google.common.";

  private ApiSignatureTestUtils() {}

  /**
   * Verifies that none of the given classes declare type parameters, supertypes, fields,
   * constructors, or methods exposing Guava ({@code com.google.common.*}) types in their generic
   * signatures, type bounds, or {@code throws} clauses.
   */
  static void assertNoGuavaTypesInSignatures(Class<?>... classes) {
    checkNotNull(classes, "classes must not be null");
    checkArgument(classes.length > 0, "classes must not be empty");
    for (Class<?> clazz : classes) {
      checkNotNull(clazz, "class element must not be null");
      checkNoGuavaTypes(clazz, "class type parameters", clazz.getTypeParameters());
      if (clazz.getGenericSuperclass() != null) {
        assertNoGuavaType(clazz, "superclass", clazz.getGenericSuperclass());
      }
      checkNoGuavaTypes(clazz, "interfaces", clazz.getGenericInterfaces());
      for (Field field : clazz.getDeclaredFields()) {
        assertNoGuavaType(clazz, field.toString(), field.getGenericType());
      }
      for (Constructor<?> ctor : clazz.getDeclaredConstructors()) {
        checkNoGuavaTypes(clazz, ctor.toString(), ctor.getTypeParameters());
        checkNoGuavaTypes(clazz, ctor.toString(), ctor.getGenericParameterTypes());
        checkNoGuavaTypes(clazz, ctor.toString(), ctor.getGenericExceptionTypes());
      }
      for (Method method : clazz.getDeclaredMethods()) {
        checkNoGuavaTypes(clazz, method.toString(), method.getTypeParameters());
        assertNoGuavaType(clazz, method.toString(), method.getGenericReturnType());
        checkNoGuavaTypes(clazz, method.toString(), method.getGenericParameterTypes());
        checkNoGuavaTypes(clazz, method.toString(), method.getGenericExceptionTypes());
      }
    }
  }

  static void assertNoGuavaType(Class<?> owner, String member, Type type) {
    Set<Type> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    checkNoGuavaTypeRecursive(owner, member, type, visited);
  }

  private static void checkNoGuavaTypes(Class<?> owner, String member, Type[] types) {
    for (Type type : types) {
      assertNoGuavaType(owner, member, type);
    }
  }

  private static void checkNoGuavaTypeRecursive(
      Class<?> owner, String member, Type type, Set<Type> visited) {
    if (type == null || !visited.add(type)) {
      return;
    }
    if (type instanceof Class<?>) {
      Class<?> rawClass = (Class<?>) type;
      if (rawClass.getName().startsWith(GUAVA_PACKAGE_PREFIX)) {
        failGuavaLeak(owner, member, rawClass.getName());
      }
      if (rawClass.isArray()) {
        checkNoGuavaTypeRecursive(owner, member, rawClass.getComponentType(), visited);
      }
      return;
    }
    if (type instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType) type;
      checkNoGuavaTypeRecursive(owner, member, parameterizedType.getRawType(), visited);
      checkNoGuavaTypeRecursive(owner, member, parameterizedType.getOwnerType(), visited);
      for (Type arg : parameterizedType.getActualTypeArguments()) {
        checkNoGuavaTypeRecursive(owner, member, arg, visited);
      }
      return;
    }
    if (type instanceof WildcardType) {
      WildcardType wildcardType = (WildcardType) type;
      for (Type upper : wildcardType.getUpperBounds()) {
        checkNoGuavaTypeRecursive(owner, member, upper, visited);
      }
      for (Type lower : wildcardType.getLowerBounds()) {
        checkNoGuavaTypeRecursive(owner, member, lower, visited);
      }
      return;
    }
    if (type instanceof GenericArrayType) {
      GenericArrayType arrayType = (GenericArrayType) type;
      checkNoGuavaTypeRecursive(owner, member, arrayType.getGenericComponentType(), visited);
      return;
    }
    if (type instanceof TypeVariable<?>) {
      TypeVariable<?> typeVar = (TypeVariable<?>) type;
      for (Type bound : typeVar.getBounds()) {
        checkNoGuavaTypeRecursive(owner, member, bound, visited);
      }
      return;
    }
    if (type.getTypeName().contains(GUAVA_PACKAGE_PREFIX)) {
      failGuavaLeak(owner, member, type.getTypeName());
    }
  }

  private static void failGuavaLeak(Class<?> owner, String member, String leakedTypeName) {
    throw new AssertionError(
        "Guava type " + leakedTypeName + " leaked in " + member + " on " + owner.getName());
  }
}
