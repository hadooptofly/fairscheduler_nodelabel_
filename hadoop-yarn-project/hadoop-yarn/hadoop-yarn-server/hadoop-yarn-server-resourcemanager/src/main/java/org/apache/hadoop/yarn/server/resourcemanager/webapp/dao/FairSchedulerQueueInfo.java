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
  private NoNullHashMap<String, Float> fractionMemUsed;
  @XmlTransient
  private NoNullHashMap<String, Float> fractionMemSteadyFairShare;
  @XmlTransient
  private NoNullHashMap<String, Float> fractionMemFairShare;
  @XmlTransient
  private NoNullHashMap<String, Float> fractionMemMinShare;
  @XmlTransient
  private NoNullHashMap<String, Float> fractionMemMaxShare;
  
  private ResourceInfo minResources;
  private ResourceInfo maxResources;
  private ResourceInfo usedResources;
  private ResourceInfo steadyFairResources;
  private ResourceInfo fairResources;
  private ResourceInfo clusterResources;
  private NoNullHashMap<String, Resource> usedResource;
  
  private String queueName;
  private String schedulingPolicy;
  
  private Collection<FairSchedulerQueueInfo> childQueues;
  
  public FairSchedulerQueueInfo() {
  }
  
  public FairSchedulerQueueInfo(FSQueue queue, FairScheduler scheduler) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();

    this.usedResource = queue.getResourceUsage();
    queueName = queue.getName();
    schedulingPolicy = queue.getPolicy().getName();

    fractionMemUsed = new NoNullHashMap<String, Float>(){};
    fractionMemSteadyFairShare = new NoNullHashMap<String, Float>(){};
    fractionMemFairShare = new NoNullHashMap<String, Float>(){};
    fractionMemMinShare = new NoNullHashMap<String, Float>(){};
    fractionMemMaxShare = new NoNullHashMap<String, Float>(){};

    clusterResources = new ResourceInfo(scheduler.getClusterResource());
    usedResources = new ResourceInfo(queue.getResourceUsage());
    steadyFairResources = new ResourceInfo(queue.getSteadyFairShare());
    fairResources = new ResourceInfo(queue.getFairShare());
    minResources = new ResourceInfo(queue.getMinShare());
    Map<String, Resource> resource = new HashMap<String, Resource>();
    for (String nodeLabel : queue.getMaxShare().keySet()) {
      Resource resource1 = scheduler.getClusterResource().get(nodeLabel);
      if (resource1 == null) {
        resource.put(nodeLabel, Resources.componentwiseMin(
            queue.getMaxShare().get(nodeLabel),
            Resources.none()));
      } else {
        resource.put(nodeLabel, scheduler
            .getClusterResource().get(nodeLabel));
      }
    }
    maxResources = new ResourceInfo(resource);

    for (String nodeLabel : queue.getResourceUsage().keySet()) {
      fractionMemUsed.put(nodeLabel, (float)usedResources.getMemory(nodeLabel) /
          clusterResources.getMemory(nodeLabel));
      fractionMemFairShare.put(nodeLabel, (float) fairResources.getMemory(nodeLabel)
          / clusterResources.getMemory(nodeLabel));
    }

    for (String nodeLabel : queue.getMaxShare().keySet()) {
      fractionMemSteadyFairShare.put(nodeLabel, (float) steadyFairResources
          .getMemory(nodeLabel) / clusterResources.getMemory(nodeLabel));
      fractionMemMinShare.put(nodeLabel, (float) minResources
          .getMemory(nodeLabel) / clusterResources.getMemory(nodeLabel));
      fractionMemMaxShare.put(nodeLabel, (float) maxResources
          .getMemory(nodeLabel) / clusterResources.getMemory(nodeLabel));
    }

    maxApps = allocConf.getQueueMaxApps(queueName);

    childQueues = new ArrayList<FairSchedulerQueueInfo>();
    if (allocConf.isReservable(queueName) &&
        !allocConf.getShowReservationAsQueues(queueName)) {
      return;
    }

    Collection<FSQueue> children = queue.getChildQueues();
    for (FSQueue child : children) {
      if (child instanceof FSLeafQueue) {
        childQueues.add(new FairSchedulerLeafQueueInfo((FSLeafQueue)child, scheduler));
      } else {
        childQueues.add(new FairSchedulerQueueInfo(child, scheduler));
      }
    }
  }
  
  /**
   * Returns the steady fair share as a fraction of the entire cluster capacity.
   */
  public float getSteadyFairShareMemoryFraction(String nodeLabel) {
    return fractionMemSteadyFairShare.get(nodeLabel);
  }

  /**
   * Returns the fair share as a fraction of the entire cluster capacity.
   */
  public float getFairShareMemoryFraction(String nodeLabel) {
    return fractionMemFairShare.get(nodeLabel);
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
  
  public ResourceInfo getUsedResources() {
    return usedResources;
  }
  
  /**
   * Returns the queue's min share in as a fraction of the entire
   * cluster capacity.
   */
  public float getMinShareMemoryFraction(String nodeLabel) {
    return fractionMemMinShare.get(nodeLabel);
  }
  
  /**
   * Returns the memory used by this queue as a fraction of the entire 
   * cluster capacity.
   */
  public float getUsedMemoryFraction(String nodeLabel) {
    return fractionMemUsed.get(nodeLabel);
  }
  
  /**
   * Returns the capacity of this queue as a fraction of the entire cluster 
   * capacity.
   */
  public float getMaxResourcesFraction(String nodeLabel) {
    return fractionMemMaxShare.get(nodeLabel);
  }
  
  /**
   * Returns the name of the scheduling policy used by this queue.
   */
  public String getSchedulingPolicy() {
    return schedulingPolicy;
  }
  
  public Collection<FairSchedulerQueueInfo> getChildQueues() {
    return childQueues;
  }

  public Map<String, Resource> getUsedResource() {
    return usedResource;
  }
}
