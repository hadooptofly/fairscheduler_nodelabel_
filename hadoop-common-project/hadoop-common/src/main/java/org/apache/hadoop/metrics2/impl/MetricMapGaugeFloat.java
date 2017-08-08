/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2.impl;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsVisitor;
import org.apache.hadoop.tools.metrics.NoNullHashMap;

public class MetricMapGaugeFloat extends AbstractMetric {
  final NoNullHashMap<String, MetricGaugeFloat> value;

  public MetricMapGaugeFloat(MetricsInfo info, NoNullHashMap<String, MetricGaugeFloat> value) {
    super(info);
    this.value = value;
  }

  public NoNullHashMap<String, MetricGaugeFloat> getValue() {
    return value;
  }

  public Float value(String label) {
    return value.get(label, info()).value();
  }

  // no useful
  @Override
  public Float value() {
    return 0.0f;
  }

  @Override
  public MetricType type() {
    return MetricType.GAUGE;
  }

  @Override
  public void visit(MetricsVisitor visitor) {
    visitor.gauge(this, value(""));
  }
}
