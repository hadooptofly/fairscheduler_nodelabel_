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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

import java.util.HashMap;
import java.util.Map;

@Metrics(context="yarn")
public class FSQueueMetrics extends QueueMetrics {

  @Metric("Fair share of memory in MB") Map<String, MutableGaugeInt> fairShareMB;
  @Metric("Fair share of CPU in vcores") Map<String, MutableGaugeInt> fairShareVCores;
  @Metric("Fair share of GPU in gcores") Map<String, MutableGaugeInt> fairShareGCores;
  @Metric("Steady fair share of memory in MB") Map<String, MutableGaugeInt> steadyFairShareMB;
  @Metric("Steady fair share of CPU in vcores") Map<String, MutableGaugeInt> steadyFairShareVCores;
  @Metric("Steady fair share of GPU in gcores") Map<String, MutableGaugeInt> steadyFairShareGCores;
  @Metric("Minimum share of memory in MB") Map<String, MutableGaugeInt> minShareMB;
  @Metric("Minimum share of CPU in vcores") Map<String, MutableGaugeInt> minShareVCores;
  @Metric("Minimum share of GPU in gcores") Map<String, MutableGaugeInt> minShareGCores;
  @Metric("Maximum share of memory in MB") Map<String, MutableGaugeInt> maxShareMB;
  @Metric("Maximum share of CPU in vcores") Map<String, MutableGaugeInt> maxShareVCores;
  @Metric("Maximum share of GPU in gcores") Map<String, MutableGaugeInt> maxShareGCores;

  Map<String, Resource> fairShare = new HashMap<String, Resource>();
  Map<String, Resource> steadFairShare = new HashMap<String, Resource>();
  Map<String, Resource> minShare = new HashMap<String, Resource>();
  Map<String, Resource> maxShare = new HashMap<String, Resource>();

  FSQueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    super(ms, queueName, parent, enableUserMetrics, conf);
  }
  
  public void setFairShare(Map<String, Resource> resource) {
    this.fairShare = resource;
  }
  
  public int getFairShareMB(String nodeLabel) {
    return fairShareMB.get(nodeLabel).value();
  }
  
  public int getFairShareVirtualCores(String nodeLabel) {
    return fairShareVCores.get(nodeLabel).value();
  }

  public int getFairShareGpuCores(String nodeLabel) {
    return fairShareGCores.get(nodeLabel).value();
  }

  public void setSteadyFairShare(Map<String, Resource> resource) {
    this.steadFairShare = resource;
  }

  public int getSteadyFairShareMB(String nodeLabel) {
    return steadyFairShareMB.get(nodeLabel).value();
  }

  public int getSteadyFairShareVCores(String nodeLabel) {
    return steadyFairShareVCores.get(nodeLabel).value();
  }

  public int getSteadyFairShareGCores(String nodeLabel) {
    return steadyFairShareGCores.get(nodeLabel).value();
  }

  public void setMinShare(Map<String, Resource> resource) {
    this.minShare = resource;
  }
  
  public int getMinShareMB(String nodeLabel) {
    return minShareMB.get(nodeLabel).value();
  }
  
  public int getMinShareVirtualCores(String nodeLabel) {
    return minShareVCores.get(nodeLabel).value();
  }

  public int getMinShareGpuCores(String nodeLabel) {
    return minShareGCores.get(nodeLabel).value();
  }
  
  public void setMaxShare(Map<String, Resource> resource) {
    this.maxShare = resource;
  }
  
  public int getMaxShareMB(String nodeLabel) {
    return maxShareMB.get(nodeLabel).value();
  }
  
  public int getMaxShareVirtualCores(String nodeLabel) {
    return maxShareVCores.get(nodeLabel).value();
  }

  public int getMaxShareGpuCores(String nodeLabel) {
    return maxShareGCores.get(nodeLabel).value();
  }
  
  public synchronized 
  static FSQueueMetrics forQueue(String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    QueueMetrics metrics = queueMetrics.get(queueName);
    if (metrics == null) {
      metrics = new FSQueueMetrics(ms, queueName, parent, enableUserMetrics, conf)
          .tag(QUEUE_INFO, queueName);
      
      // Register with the MetricsSystems
      if (ms != null) {
        metrics = ms.register(
                sourceName(queueName).toString(), 
                "Metrics for queue: " + queueName, metrics);
      }
      queueMetrics.put(queueName, metrics);
    }

    return (FSQueueMetrics)metrics;
  }

}
