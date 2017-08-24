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


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.NoNullHashMap;
import org.apache.hadoop.yarn.util.resource.Resources;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({FairSchedulerLeafQueueInfo.class})
public class FairSchedulerQueueInfo {  
  private int maxApps;
  
  @XmlTransient
  private float fractionMemUsed;
  @XmlTransient
  private float fractionMemSteadyFairShare;
  @XmlTransient
  private float fractionMemFairShare;
  @XmlTransient
  private float fractionMemMinShare;
  @XmlTransient
  private float fractionMemMaxShare;
  
  private ResourceInfo minResources;
  private ResourceInfo maxResources;
  private ResourceInfo usedResources;
  private ResourceInfo steadyFairResources;
  private ResourceInfo fairResources;
  private ResourceInfo clusterResources;
  private NoNullHashMap<String, Resource> usedResource;
  
  private String queueName;
  public FairSchedulerQueueInfoList queues;
  private String schedulingPolicy;

  public FairSchedulerQueueInfo() {
  }
  
  public FairSchedulerQueueInfo(FSQueue queue, FairScheduler scheduler, String nodeLabel) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();

    this.usedResource = queue.getResourceUsage();
    queueName = queue.getName();
    schedulingPolicy = queue.getPolicy().getName();

    clusterResources = new ResourceInfo(scheduler.getClusterResource());
    usedResources = new ResourceInfo(queue.getResourceUsage());
    steadyFairResources = new ResourceInfo(queue.getSteadyFairShare());
    fairResources = new ResourceInfo(queue.getFairShare());
    minResources = new ResourceInfo(queue.getMinShare());
    Map<String, Resource> resource = new NoNullHashMap<String, Resource>(){};
    for (String label : queue.getMaxShare().keySet()) {
      Resource resource1 = scheduler.getClusterResource().get(label);
      if (resource1 == null) {
        resource.put(label, Resources.componentwiseMin(
            queue.getMaxShare().get(label),
            Resources.none()));
      } else {
        resource.put(label, scheduler
            .getClusterResource().get(label));
      }
    }
    maxResources = new ResourceInfo(resource);

    fractionMemUsed = (float)usedResources.getMemory(nodeLabel) /
        clusterResources.getMemory(nodeLabel);
    fractionMemFairShare = (float) fairResources.getMemory(nodeLabel)
        / clusterResources.getMemory(nodeLabel);

    fractionMemSteadyFairShare = steadyFairResources
        .getMemory(nodeLabel) / clusterResources.getMemory(nodeLabel);
    fractionMemMinShare = minResources
        .getMemory(nodeLabel) / clusterResources.getMemory(nodeLabel);
    fractionMemMaxShare = maxResources
        .getMemory(nodeLabel) / clusterResources.getMemory(nodeLabel);

    maxApps = allocConf.getQueueMaxApps(queueName);

    if (allocConf.isReservable(queueName) &&
        !allocConf.getShowReservationAsQueues(queueName)) {
      return;
    }
  }
  
  /**
   * Returns the steady fair share as a fraction of the entire cluster capacity.
   */
  public float getSteadyFairShareMemoryFraction() {
    return fractionMemSteadyFairShare;
  }

  /**
   * Returns the fair share as a fraction of the entire cluster capacity.
   */
  public float getFairShareMemoryFraction() {
    return fractionMemFairShare;
  }

  /**
   * Returns the steady fair share of this queue in megabytes.
   */
  public ResourceInfo getSteadyFairShare() {
    return steadyFairResources;
  }

  /**
   * Returns the fair share of this queue in megabytes
   */
  public ResourceInfo getFairShare() {
    return fairResources;
  }

  public ResourceInfo getMinResources() {
    return minResources;
  }
  
  public ResourceInfo getMaxResources() {
    return maxResources;
  }
  
  public int getMaxApplications() {
    return maxApps;
  }
  
  public String getQueueName() {
    return queueName;
  }

  public FairSchedulerQueueInfoList getQueues() { return this.queues; };

  public ResourceInfo getUsedResources() {
    return usedResources;
  }
  
  /**
   * Returns the queue's min share in as a fraction of the entire
   * cluster capacity.
   */
  public float getMinShareMemoryFraction() {
    return fractionMemMinShare;
  }
  
  /**
   * Returns the memory used by this queue as a fraction of the entire 
   * cluster capacity.
   */
  public float getUsedMemoryFraction() {
    return fractionMemUsed;
  }
  
  /**
   * Returns the capacity of this queue as a fraction of the entire cluster 
   * capacity.
   */
  public float getMaxResourcesFraction() {
    return fractionMemMaxShare;
  }
  
  /**
   * Returns the name of the scheduling policy used by this queue.
   */
  public String getSchedulingPolicy() {
    return schedulingPolicy;
  }

  public Map<String, Resource> getUsedResource() {
    return usedResource;
  }
}
