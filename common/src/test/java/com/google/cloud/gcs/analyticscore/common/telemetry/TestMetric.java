package com.google.cloud.gcs.analyticscore.common.telemetry;

import java.util.Objects;

public class TestMetric implements Metric {
  private final String name;
  private final MetricType type;

  private TestMetric(String name, MetricType type) {
    this.name = name;
    this.type = type;
  }

  public static TestMetric of(String name, MetricType type) {
    return new TestMetric(name, type);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public MetricType getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Metric)) return false;
    Metric that = (Metric) o;
    return Objects.equals(name, that.getName()) && type == that.getType();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public String toString() {
    return name;
  }
}
