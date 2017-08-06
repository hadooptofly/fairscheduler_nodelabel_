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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.NoNullHashMap;
import org.apache.hadoop.yarn.util.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.ComparatorWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

/**
 * Makes scheduling decisions by trying to equalize shares of memory.
 */
@Private
@Unstable
public class FairSharePolicy extends SchedulingPolicy {
  @VisibleForTesting
  public static final String NAME = "fair";
  private static final DefaultResourceCalculator RESOURCE_CALCULATOR =
      new DefaultResourceCalculator();
  private FairShareComparator comparator = new FairShareComparator();

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Compare Schedulables via weighted fair sharing. In addition, Schedulables
   * below their min share get priority over those whose min share is met.
   * 
   * Schedulables below their min share are compared by how far below it they
   * are as a ratio. For example, if job A has 8 out of a min share of 10 tasks
   * and job B has 50 out of a min share of 100, then job B is scheduled next,
   * because B is at 50% of its min share and A is at 80% of its min share.
   * 
   * Schedulables above their min share are compared by (runningTasks / weight).
   * If all weights are equal, slots are given to the job with the fewest tasks;
   * otherwise, jobs with more weight get proportionally more slots.
   */
  private static class FairShareComparator implements ComparatorWrapper<Schedulable, String>,
      Serializable {
    private static final long serialVersionUID = 5564969375856699313L;
    private static final Resource ONE = Resources.createResource(1);

    @Override
    public int compare(Schedulable s1, Schedulable s2, String nodeLabel) {
      double minShareRatio1, minShareRatio2;
      double useToWeightRatio1, useToWeightRatio2;
      Resource minShare1 = Resources.min(RESOURCE_CALCULATOR, null,
          s1.getMinShare().get(nodeLabel), s1.getDemand().get(nodeLabel));
      Resource minShare2 = Resources.min(RESOURCE_CALCULATOR, null,
          s2.getMinShare().get(nodeLabel), s2.getDemand().get(nodeLabel));
      boolean s1Needy = Resources.lessThan(RESOURCE_CALCULATOR, null,
          s1.getResourceUsage().get(nodeLabel), minShare1);
      boolean s2Needy = Resources.lessThan(RESOURCE_CALCULATOR, null,
          s2.getResourceUsage().get(nodeLabel), minShare2);
      minShareRatio1 = (double) s1.getResourceUsage().get(nodeLabel).getMemory()
          / Resources.max(RESOURCE_CALCULATOR, null, minShare1, ONE).getMemory();
      minShareRatio2 = (double) s2.getResourceUsage().get(nodeLabel).getMemory()
          / Resources.max(RESOURCE_CALCULATOR, null, minShare2, ONE).getMemory();
      useToWeightRatio1 = s1.getResourceUsage().get(nodeLabel).getMemory() /
          s1.getWeights().get(nodeLabel).getWeight(ResourceType.MEMORY);
      useToWeightRatio2 = s2.getResourceUsage().get(nodeLabel).getMemory() /
          s2.getWeights().get(nodeLabel).getWeight(ResourceType.MEMORY);
      int res = 0;
      if (s1Needy && !s2Needy)
        res = -1;
      else if (s2Needy && !s1Needy)
        res = 1;
      else if (s1Needy && s2Needy)
        res = (int) Math.signum(minShareRatio1 - minShareRatio2);
      else
        // Neither schedulable is needy
        res = (int) Math.signum(useToWeightRatio1 - useToWeightRatio2);
      if (res == 0) {
        // Apps are tied in fairness ratio. Break the tie by submit time and job
        // name to get a deterministic ordering, which is useful for unit tests.
        res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());
        if (res == 0)
          res = s1.getName().compareTo(s2.getName());
      }
      return res;
    }
  }

  @Override
  public ComparatorWrapper<Schedulable, String> getComparator() {
    return comparator;
  }

  @Override
  public Resource getHeadroom(Resource queueFairShare,
                              Resource queueUsage, Resource maxAvailable) {
    int queueAvailableMemory = Math.max(
        queueFairShare.getMemory() - queueUsage.getMemory(), 0);
    Resource headroom = Resources.createResource(
        Math.min(maxAvailable.getMemory(), queueAvailableMemory),
        maxAvailable.getVirtualCores(), maxAvailable.getGpuCores());
    return headroom;
  }

  @Override
  public void computeShares(Collection<? extends Schedulable> schedulables,
                            NoNullHashMap<String, Resource> totalResources) {
    ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.MEMORY);
  }

  @Override
  public void computeSteadyShares(Collection<? extends FSQueue> queues,
                                  NoNullHashMap<String, Resource> totalResources) {
    ComputeFairShares.computeSteadyShares(queues, totalResources,
        ResourceType.MEMORY);
  }

  @Override
  public NoNullHashMap<String, Boolean> checkIfUsageOverFairShare(NoNullHashMap<String, Resource> usage, NoNullHashMap<String, Resource> fairShare) {
    NoNullHashMap<String, Boolean> isOver = new NoNullHashMap<String, Boolean>(){};
    for (String nodeLabel : usage.keySet()){
      if (Resources.fitsIn(usage.get(nodeLabel), fairShare.get(nodeLabel))) {
        isOver.put(nodeLabel, false);
      } else {
        isOver.put(nodeLabel, true);
      }
    }
    return isOver;
  }

  @Override
  public boolean checkIfUsageOverFairShare(Resource usage, Resource fairShare) {
    return Resources.fitsIn(usage, fairShare);
  }

  @Override
  public boolean checkIfAMResourceUsageOverLimit(Resource usage, Resource maxAMResource) {
    return usage.getMemory() > maxAMResource.getMemory();
  }

  @Override
  public byte getApplicableDepth() {
    return SchedulingPolicy.DEPTH_ANY;
  }
}
