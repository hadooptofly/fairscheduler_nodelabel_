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
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import java.util.Map;

@XmlRootElement(name = "userMetrics")
@XmlAccessorType(XmlAccessType.FIELD)
public class UserMetricsInfo {

  protected int appsSubmitted;
  protected int appsCompleted;
  protected int appsPending;
  protected int appsRunning;
  protected int appsFailed;
  protected int appsKilled;

  protected QueueMetrics userMetrics;

  @XmlTransient
  protected boolean userMetricsAvailable;

  public UserMetricsInfo() {
  } // JAXB needs this

  public UserMetricsInfo(final ResourceManager rm, final String user) {
    ResourceScheduler rs = rm.getResourceScheduler();
    QueueMetrics metrics = rs.getRootQueueMetrics();
    userMetrics = metrics.getUserMetrics(user);
    this.userMetricsAvailable = false;

    if (userMetrics != null) {
      this.userMetricsAvailable = true;

      this.appsSubmitted = userMetrics.getAppsSubmitted();
      this.appsCompleted = userMetrics.getAppsCompleted();
      this.appsPending = userMetrics.getAppsPending();
      this.appsRunning = userMetrics.getAppsRunning();
      this.appsFailed = userMetrics.getAppsFailed();
      this.appsKilled = userMetrics.getAppsKilled();
    }
  }

  public boolean metricsAvailable() {
    return userMetricsAvailable;
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
    return userMetrics.getReservedMB();
  }

  public Map<String, MutableGaugeInt> getAllocatedMB() {
    return userMetrics.getAllocatedMB();
  }

  public Map<String, MutableGaugeInt> getPendingMB() {
    return userMetrics.getPendingMB();
  }

  public Map<String, MutableGaugeInt> getReservedVirtualCores() {
    return userMetrics.getReservedVirtualCores();
  }

  public Map<String, MutableGaugeInt> getAllocatedVirtualCores() {
    return userMetrics.getAllocatedVirtualCores();
  }

  public Map<String, MutableGaugeInt> getPendingVirtualCores() {
    return userMetrics.getPendingVirtualCores();
  }

  public Map<String, MutableGaugeInt> getReservedGpuCores() {
    return userMetrics.getReservedGpuCores();
  }

  public Map<String, MutableGaugeInt> getAllocatedGpuCores() {
    return userMetrics.getAllocatedGpuCores();
  }

  public Map<String, MutableGaugeInt> getPendingGpuCores() {
    return userMetrics.getPendingGpuCores();
  }

  public Map<String, MutableGaugeInt> getReservedContainers() {
    return userMetrics.getReservedContainers();
  }

  public Map<String, MutableGaugeInt> getRunningContainers() {
    return userMetrics.getAllocatedContainers();
  }

  public Map<String, MutableGaugeInt> getPendingContainers() {
    return userMetrics.getPendingContainers();
  }
}
