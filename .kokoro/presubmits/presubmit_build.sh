#!/bin/bash

# Fail on any error.
set -e

cd "${KOKORO_ARTIFACTS_DIR}/github/gcs-analytics-core"
./mvnw clean package
./mvnw -Pintegration-test clean verify