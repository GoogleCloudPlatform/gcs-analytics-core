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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import com.google.common.collect.ImmutableList;
import java.util.Optional;

final class DataFileExtensions {

  private DataFileExtensions() {}

  static boolean hasAnyExtension(Optional<String> objectName, ImmutableList<String> extensions) {
    if (objectName.isEmpty()) {
      return false;
    }
    String name = objectName.get();
    for (int i = 0; i < extensions.size(); i++) {
      String ext = extensions.get(i);
      if (name.regionMatches(true, name.length() - ext.length(), ext, 0, ext.length())) {
        return true;
      }
    }
    return false;
  }
}
