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

import java.util.concurrent.atomic.AtomicLong;

/**
 * A mutable long gauge
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableMapGaugeLong extends MutableMapGauge {

  private NoNullHashMap<String, MutableGaugeLong> value = new NoNullHashMap<String, MutableGaugeLong>(){};

  public MutableMapGaugeLong(MetricsInfo info, NoNullHashMap<String, MutableGaugeLong> initValue) {
    super(info);
    value = initValue;
  }

  public NoNullHashMap<String, MutableGaugeLong> getValue() {
    return value;
  }

  public long value(String label) {
    return value.get(label, info()).value();
  }

  @Override
  public void incr(String label) {
    incr(label, 1);
  }

  /**
   * Increment by delta
   * @param delta of the increment
   */
  public void incr(String label, long delta) {
    value.get(label, info()).getValue().addAndGet(delta);
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
  public void decr(String label, long delta) {
    value.get(label, info()).getValue().addAndGet(-delta);
    setChanged();
  }

  /**
   * Set the value of the metric
   * @param label to set
   * @param value to set
   */
  public void set(String label, long value) {
    this.value.get(label, info()).set(value);
    setChanged();
  }

  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || changed()) {
      builder.addGauge(info(), value(""));
      clearChanged();
    }
  }

}
