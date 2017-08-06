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

package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.tools.metrics.NoNullHashMap;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A mutable int gauge
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableMapGaugeInt extends MutableMapGauge {

  private NoNullHashMap<String, MutableGaugeInt> value = new NoNullHashMap<String, MutableGaugeInt>(){};

  public MutableMapGaugeInt(MetricsInfo info, NoNullHashMap<String, MutableGaugeInt> initValue) {
    super(info);
    value = initValue;
  }

  public NoNullHashMap<String, MutableGaugeInt> getValue() {
    return value;
  }

  public int value(String label) {
    return value.get(label).value();
  }

  @Override
  public void incr(String label) {
    incr(label, 1);
  }

  /**
   * Increment by delta
   * @param delta of the increment
   */
  public void incr(String label, int delta) {
    value.get(label).getValue().addAndGet(delta);
    setChanged();
  }

  @Override
  public void decr(String label) {
    decr(label, 1);
  }

  /**
   * decrement by delta
   * @param delta of the decrement
   */
  public void decr(String label, int delta) {
    value.get(label).getValue().addAndGet(-delta);
    setChanged();
  }

  /**
   * Set the value of the metric
   * @param label to set
   * @param value to set
   */
  public void set(String label, int value) {
    this.value.get(label).set(value);
    setChanged();
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || changed()) {
      builder.addGauge(info(), value(""));
      clearChanged();
    }
  }
}
