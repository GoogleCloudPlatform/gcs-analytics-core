/*
 * Copyright 2026 Google LLC
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

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class GcsReadChannelBenchmarkState {

    public enum SeekScenario {
        SEEK_128K_LIMIT_0(128 * 1024, 0),
        SEEK_128K_LIMIT_128K(128 * 1024, 128 * 1024),
        SEEK_512K_LIMIT_0(512 * 1024, 0),
        SEEK_512K_LIMIT_512K(512 * 1024, 512 * 1024),
        SEEK_1M_LIMIT_0(1024 * 1024, 0),
        SEEK_1M_LIMIT_1M(1024 * 1024, 1024 * 1024);

        private final int seekDistance;
        private final int inplaceSeekLimit;

        SeekScenario(int seekDistance, int inplaceSeekLimit) {
            this.seekDistance = seekDistance;
            this.inplaceSeekLimit = inplaceSeekLimit;
        }

        public int getSeekDistance() {
            return seekDistance;
        }

        public int getInplaceSeekLimit() {
            return inplaceSeekLimit;
        }
    }

    @Param({
        "SEEK_128K_LIMIT_0",
        "SEEK_128K_LIMIT_128K",
        "SEEK_512K_LIMIT_0",
        "SEEK_512K_LIMIT_512K",
        "SEEK_1M_LIMIT_0",
        "SEEK_1M_LIMIT_1M"
    })
    public SeekScenario scenario;
}
