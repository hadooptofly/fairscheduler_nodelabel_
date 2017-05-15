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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collection;
import java.util.Comparator;

import static org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType.*;

/**
 * Makes scheduling decisions by trying to equalize gpu resource usage.
 * A schedulable's gpu resource usage is the largest ratio of resource
 * usage to capacity among the resource types it is using.
 */
@Private
@Unstable
public class FairShareGPUPolicy extends SchedulingPolicy {

  public static final String NAME = "fair4gpu";

  private FairShareGPUComparator comparator =
      new FairShareGPUComparator();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public byte getApplicableDepth() {
    return SchedulingPolicy.DEPTH_ANY;
  }

  @Override
  public Comparator<Schedulable> getComparator() {
    return comparator;
  }
  
  @Override
  public void computeShares(Collection<? extends Schedulable> schedulables,
      Resource totalResources) {
    for (ResourceType type : ResourceType.values()) {
      ComputeFairShares.computeShares(schedulables, totalResources, type);
    }
  }

  @Override
  public void computeSteadyShares(Collection<? extends FSQueue> queues,
      Resource totalResources) {
    for (ResourceType type : ResourceType.values()) {
      ComputeFairShares.computeSteadyShares(queues, totalResources, type);
    }
  }

  @Override
  public boolean checkIfUsageOverFairShare(Resource usage, Resource fairShare) {
    return !Resources.fitsIn(usage, fairShare);
  }

  @Override
  public boolean checkIfAMResourceUsageOverLimit(Resource usage, Resource maxAMResource) {
    return !Resources.fitsIn(usage, maxAMResource);
  }

  @Override
  public Resource getHeadroom(Resource queueFairShare, Resource queueUsage,
                              Resource maxAvailable) {
    int queueAvailableMemory =
        Math.max(queueFairShare.getMemory() - queueUsage.getMemory(), 0);
    int queueAvailableCPU =
        Math.max(queueFairShare.getVirtualCores() - queueUsage
            .getVirtualCores(), 0);
    int queueAvailableGPU =
            Math.max(queueFairShare.getGpuCores() - queueUsage.getGpuCores(), 0);
    Resource headroom = Resources.createResource(
        Math.min(maxAvailable.getMemory(), queueAvailableMemory),
        Math.min(maxAvailable.getVirtualCores(),
            queueAvailableCPU),
                Math.min(maxAvailable.getGpuCores(), queueAvailableGPU));
    return headroom;
  }


  public static class FairShareGPUComparator implements Comparator<Schedulable> {

    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      Resource minShare1 = s1.getMinShare().equals(Resources.none()) ? s1.getDemand() :
          s1.getDemand().getGpuCores() > s1.getMinShare().getGpuCores() ? s1.getMinShare() : s1.getDemand();
      Resource minShare2 = s2.getMinShare().equals(Resources.none()) ? s2.getDemand() :
          s2.getDemand().getGpuCores() > s2.getMinShare().getGpuCores() ? s2.getMinShare() : s2.getDemand();

      double minshareRatio1 = (double) s1.getResourceUsage().getGpuCores()/minShare1.getGpuCores();
      double minshareRatio2 = (double) s2.getResourceUsage().getGpuCores()/ minShare2.getGpuCores();

      double useToWeightRatio1 = s1.getResourceUsage().getGpuCores()/s1.getWeights().getWeight(GPU);
      double useToWeightRatio2 = s2.getResourceUsage().getGpuCores()/s2.getWeights().getWeight(GPU);

      // A queue is needy for its min share if its dominant resource
      // (with respect to the cluster capacity) is below its configured min share
      // for that resource
      boolean s1Needy = minshareRatio1 < 1.0f;
      boolean s2Needy = minshareRatio2 < 1.0f;
      
      int res = 0;
      if (!s2Needy && !s1Needy) {
        res = (int) Math.signum(useToWeightRatio1 - useToWeightRatio2);
      } else if (s1Needy && !s2Needy) {
        res = -1;
      } else if (s2Needy && !s1Needy) {
        res = 1;
      } else { // both are needy below min share
        res = (int) Math.signum(minshareRatio1 - minshareRatio2);
      }
      if (res == 0) {
        // Apps are tied in fairness ratio. Break the tie by submit time.
        res = (int)(s1.getStartTime() - s2.getStartTime());
        if (res == 0)
          res = s1.getName().compareTo(s2.getName());
      }

      return res;
    }
  }
}
