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
package com.google.cloud.gcs.analyticscore.common.telemetry;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.LegacyAbstractLogger;
import org.slf4j.helpers.MessageFormatter;

/**
 * An SLF4J {@link org.slf4j.Logger} that records what it was asked to log, and reports every level
 * as either enabled or disabled.
 *
 * <p>A fake rather than a mock: it routes through SLF4J's own fluent-API machinery, so tests see
 * the message exactly as a real backend would receive it, including whether deferred arguments were
 * resolved. It also makes the tests independent of whichever binding happens to be on the test
 * classpath, so they cannot be silently disarmed by logging configuration.
 */
final class RecordingLogger extends LegacyAbstractLogger {

  private final boolean enabled;
  private final List<Level> levels = new ArrayList<>();
  private final List<String> messages = new ArrayList<>();

  private RecordingLogger(boolean enabled) {
    this.enabled = enabled;
    this.name = RecordingLogger.class.getName();
  }

  static RecordingLogger enabled() {
    return new RecordingLogger(true);
  }

  static RecordingLogger disabled() {
    return new RecordingLogger(false);
  }

  /** The levels at which this logger was invoked, in order. */
  List<Level> getLevels() {
    return levels;
  }

  /** The fully formatted messages this logger was invoked with, in order. */
  List<String> getMessages() {
    return messages;
  }

  @Override
  protected void handleNormalizedLoggingCall(
      Level level, Marker marker, String messagePattern, Object[] arguments, Throwable throwable) {
    levels.add(level);
    messages.add(MessageFormatter.basicArrayFormat(messagePattern, arguments));
  }

  @Override
  protected String getFullyQualifiedCallerName() {
    return null;
  }

  @Override
  public boolean isTraceEnabled() {
    return enabled;
  }

  @Override
  public boolean isDebugEnabled() {
    return enabled;
  }

  @Override
  public boolean isInfoEnabled() {
    return enabled;
  }

  @Override
  public boolean isWarnEnabled() {
    return enabled;
  }

  @Override
  public boolean isErrorEnabled() {
    return enabled;
  }
}
