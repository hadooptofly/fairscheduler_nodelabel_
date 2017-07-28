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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import java.util.Map;

@XmlRootElement(name = "clusterMetrics")
@XmlAccessorType(XmlAccessType.FIELD)
public class ClusterMetricsInfo {

  protected int appsSubmitted;
  protected int appsCompleted;
  protected int appsPending;
  protected int appsRunning;
  protected int appsFailed;
  protected int appsKilled;

  protected Map<String, Long> totalMB;
  protected Map<String, Integer> totalVirtualCores;
  protected Map<String, Integer> totalGpuCores;
  protected int totalNodes;
  protected int lostNodes;
  protected int unhealthyNodes;
  protected int decommissionedNodes;
  protected int rebootedNodes;
  protected int activeNodes;

  protected QueueMetrics metrics;

  public ClusterMetricsInfo() {
  } // JAXB needs this

  public ClusterMetricsInfo(final ResourceManager rm) {
    ResourceScheduler rs = rm.getResourceScheduler();
    metrics = rs.getRootQueueMetrics();
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();

    this.appsSubmitted = metrics.getAppsSubmitted();
    this.appsCompleted = metrics.getAppsCompleted();
    this.appsPending = metrics.getAppsPending();
    this.appsRunning = metrics.getAppsRunning();
    this.appsFailed = metrics.getAppsFailed();
    this.appsKilled = metrics.getAppsKilled();

    this.activeNodes = clusterMetrics.getNumActiveNMs();
    this.lostNodes = clusterMetrics.getNumLostNMs();
    this.unhealthyNodes = clusterMetrics.getUnhealthyNMs();
    this.decommissionedNodes = clusterMetrics.getNumDecommisionedNMs();
    this.rebootedNodes = clusterMetrics.getNumRebootedNMs();
    this.totalNodes = activeNodes + lostNodes + decommissionedNodes
        + rebootedNodes + unhealthyNodes;

    for (String nodeLabel : rm.getRMContext().getNodeLabelManager().getLabelSet()) {
      MutableGaugeInt availMB = metrics.getAvailableMB().get(nodeLabel);
      MutableGaugeInt allocateMB = metrics.getAllocatedMB().get(nodeLabel);
      MutableGaugeInt availVcore = metrics.getAvailableVirtualCores().get(nodeLabel);
      MutableGaugeInt allocateVcore = metrics.getAllocatedGpuCores().get(nodeLabel);
      MutableGaugeInt availGcore = metrics.getAvailableGpuCores().get(nodeLabel);
      MutableGaugeInt allocateGcore = metrics.getAllocatedGpuCores().get(nodeLabel);

      this.totalMB.put(nodeLabel, (long)(availMB != null ? availMB.value() : 0
              + (allocateMB != null ? allocateMB.value() : 0)));
      this.totalVirtualCores.put(nodeLabel, availVcore != null ? availVcore.value() : 0
              + (allocateVcore != null ? allocateVcore.value() : 0));
      this.totalGpuCores.put(nodeLabel, availGcore != null ? availGcore.value() : 0
              + (allocateGcore != null ? allocateGcore.value() : 0));
    }
  }

  public int getAppsSubmitted() {
    return this.appsSubmitted;
  }

  public int getAppsCompleted() {
    return appsCompleted;
  }

  public int getAppsPending() {
    return appsPending;
  }

  public int getAppsRunning() {
    return appsRunning;
  }

  public int getAppsFailed() {
    return appsFailed;
  }

  public int getAppsKilled() {
    return appsKilled;
  }

  public Map<String, MutableGaugeInt> getReservedMB() {
    return metrics.getReservedMB();
  }

  public Map<String, MutableGaugeInt> getAvailableMB() {
    return metrics.getAvailableMB();
  }

  public Map<String, MutableGaugeInt> getAllocatedMB() {
    return metrics.getAllocatedMB();
  }

  public Map<String, MutableGaugeInt> getReservedVirtualCores() {
    return metrics.getReservedVirtualCores();
  }

  public Map<String, MutableGaugeInt> getAvailableVirtualCores() {
    return metrics.getAvailableVirtualCores();
  }

  public Map<String, MutableGaugeInt> getAllocatedVirtualCores() {
    return metrics.getAllocatedVirtualCores();
  }

  public Map<String, MutableGaugeInt> getReservedGpuCores() {
    return metrics.getReservedGpuCores();
  }

  public Map<String, MutableGaugeInt> getAvailableGpuCores() {
    return metrics.getAvailableGpuCores();
  }

  public Map<String, MutableGaugeInt> getAllocatedGpuCores() {
    return metrics.getAllocatedGpuCores();
  }

  public Map<String, MutableGaugeInt> getContainersAllocated() {
    return metrics.getAllocatedContainers();
  }

  public Map<String, MutableGaugeInt> getReservedContainers() {
    return metrics.getReservedContainers();
  }

  public Map<String, MutableGaugeInt> getPendingContainers() {
    return metrics.getPendingContainers();
  }

  public Map<String, Long> getTotalMB() {
    return this.totalMB;
  }

  public Map<String, Integer> getTotalVirtualCores() {
    return this.totalVirtualCores;
  }

  public Map<String, Integer> getTotalGpuCores() {
    return this.totalGpuCores;
  }

  public int getTotalNodes() {
    return this.totalNodes;
  }

  public int getActiveNodes() {
    return this.activeNodes;
  }

  public int getLostNodes() {
    return this.lostNodes;
  }

  public int getRebootedNodes() {
    return this.rebootedNodes;
  }

  public int getUnhealthyNodes() {
    return this.unhealthyNodes;
  }

  public int getDecommissionedNodes() {
    return this.decommissionedNodes;
  }

}
