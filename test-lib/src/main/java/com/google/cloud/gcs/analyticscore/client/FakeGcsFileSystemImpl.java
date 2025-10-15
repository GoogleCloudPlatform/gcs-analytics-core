/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FakeGcsFileSystemImpl extends GcsFileSystemImpl {
  public FakeGcsFileSystemImpl(GcsFileSystemOptions fileSystemOptions) {
    super(initializeGcsClient(fileSystemOptions), fileSystemOptions);
  }

  private static GcsClient initializeGcsClient(GcsFileSystemOptions options) {
    Supplier<ExecutorService> executorServiceSupplier =
        Suppliers.ofInstance(Executors.newCachedThreadPool());
    return new FakeGcsClientImpl(options.getGcsClientOptions(), executorServiceSupplier);
  }
}
