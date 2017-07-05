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

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.HashMap;
import java.util.Map;

/**
 * Dummy implementation of Schedulable for unit testing.
 */
public class FakeSchedulable implements Schedulable {
  private Map<String, Resource> usage;
  private Map<String, Resource> minShare;
  private Map<String, Resource> maxShare;
  private Map<String, Resource> fairShare;
  private Map<String, ResourceWeights> weights;
  private Priority priority;
  private long startTime;

  static private Map<String, Resource> getResource(int size, String nodeLabel) {
    Map<String, Resource> resourceMap = new HashMap<String, Resource>();
    resourceMap.put(nodeLabel, Resources.createResource(size, 0));
    return resourceMap;
  }

  static private Map<String, ResourceWeights> getResourceWeights(double size, String nodeLabel) {
    Map<String, ResourceWeights> resourceMap = new HashMap<String, ResourceWeights>();
    resourceMap.put(nodeLabel, new ResourceWeights((float) size));
    return resourceMap;
  }

  public FakeSchedulable() {
    this(0, Integer.MAX_VALUE, 1, 0, 0, 0);
  }
  
  public FakeSchedulable(int minShare) {
    this(minShare, Integer.MAX_VALUE, 1, 0, 0, 0);
  }
  
  public FakeSchedulable(int minShare, int maxShare) {
    this(minShare, maxShare, 1, 0, 0, 0);
  }
  
  public FakeSchedulable(int minShare, double memoryWeight) {
    this(minShare, Integer.MAX_VALUE, memoryWeight, 0, 0, 0);
  }
  
  public FakeSchedulable(int minShare, int maxShare, double memoryWeight) {
    this(minShare, maxShare, memoryWeight, 0, 0, 0);
  }
  
  public FakeSchedulable(int minShare, int maxShare, double weight, int fairShare, int usage,
      long startTime) {
    this(getResource(minShare, ""), getResource(maxShare, ""),
        getResourceWeights(weight, ""), getResource(fairShare, ""),
        getResource(usage, ""), startTime);
  }
  
  public FakeSchedulable(Resource minShare, ResourceWeights weights) {
    this(getResource(minShare.getMemory(), ""),
        getResource(Integer.MAX_VALUE, ""),
        getResourceWeights(weights.getWeight(ResourceType.MEMORY), ""),
        getResource(0, ""),
        getResource(0, ""),
        0);
  }
  
  public FakeSchedulable(Map<String, Resource> minShare, Map<String, Resource> maxShare,
                         Map<String, ResourceWeights> weight, Map<String, Resource> fairShare,
                         Map<String, Resource> usage, long startTime) {
    this.minShare = minShare;
    this.maxShare = maxShare;
    this.weights = weight;
    setFairShare(fairShare);
    this.usage = usage;
    this.priority = Records.newRecord(Priority.class);
    this.startTime = startTime;
  }
  
  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    return null;
  }

  @Override
  public RMContainer preemptContainer(String nodeLabel) {
    return null;
  }

  @Override
  public Map<String, Resource> getFairShare() {
    return this.fairShare;
  }

  @Override
  public void setFairShare(Map<String, Resource> fairShare) {
    this.fairShare = fairShare;
  }

  @Override
  public Map<String, Resource> getDemand() {
    return null;
  }

  @Override
  public String getName() {
    return "FakeSchedulable" + this.hashCode();
  }

  @Override
  public Priority getPriority() {
    return priority;
  }

  @Override
  public Map<String, Resource> getResourceUsage() {
    return usage;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }
  
  @Override
  public Map<String, ResourceWeights> getWeights() {
    return weights;
  }
  
  @Override
  public Map<String, Resource> getMinShare() {
    return minShare;
  }
  
  @Override
  public Map<String, Resource> getMaxShare() {
    return maxShare;
  }

  @Override
  public void updateDemand() {}
}
