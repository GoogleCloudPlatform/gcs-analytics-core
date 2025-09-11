#!/bin/bash

# Fail on any error.
set -e

cd "${KOKORO_ARTIFACTS_DIR}/github/gcs-analytics-core"
./mvnw -Pintegration-test clean verify -Dmaven.javadoc.skip=true -Dsource.skip=true  -Dgpg.skip=true