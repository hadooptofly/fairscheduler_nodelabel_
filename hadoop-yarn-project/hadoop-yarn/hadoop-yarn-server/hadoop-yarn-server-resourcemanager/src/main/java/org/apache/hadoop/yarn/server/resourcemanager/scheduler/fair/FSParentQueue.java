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

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

@Private
@Unstable
public class FSParentQueue extends FSQueue {
  private static final Log LOG = LogFactory.getLog(
      FSParentQueue.class.getName());

  private final List<FSQueue> childQueues = 
      new ArrayList<FSQueue>();
  private Map<String, Resource> demand = new HashMap<String, Resource>();
  private int runnableApps;
  
  public FSParentQueue(String name, FairScheduler scheduler,
      FSParentQueue parent) {
    super(name, scheduler, parent);
  }
  
  public void addChildQueue(FSQueue child) {
    childQueues.add(child);
  }

  @Override
  public void recomputeShares() {
    policy.computeShares(childQueues, getFairShare());
    for (FSQueue childQueue : childQueues) {
      childQueue.getMetrics().setFairShare(childQueue.getFairShare());
      childQueue.recomputeShares();
    }
  }

  public void recomputeSteadyShares() {
    policy.computeSteadyShares(childQueues, getSteadyFairShare());
    for (FSQueue childQueue : childQueues) {
      childQueue.getMetrics().setSteadyFairShare(childQueue.getSteadyFairShare());
      if (childQueue instanceof FSParentQueue) {
        ((FSParentQueue) childQueue).recomputeSteadyShares();
      }
    }
  }

  @Override
  public void updatePreemptionVariables() {
    super.updatePreemptionVariables();
    // For child queues
    for (FSQueue childQueue : childQueues) {
      childQueue.updatePreemptionVariables();
    }
  }

  @Override
  public Map<String, Resource> getDemand() {
    return demand;
  }

  @Override
  public Map<String, Resource> getResourceUsage() {
    Map<String, Resource> usage = new HashMap<String, Resource>();
    for (FSQueue child : childQueues) {
      Map<String, Resource> use = child.getResourceUsage();
      for (Map.Entry<String, Resource> labelUsage : use.entrySet()) {
        if (!usage.containsKey(labelUsage.getKey())) {
          usage.put(labelUsage.getKey(), Resources.clone(labelUsage.getValue()));
        } else {
          usage.put(labelUsage.getKey(),
              Resources.addTo(usage.get(labelUsage.getKey()),
                  labelUsage.getValue()));
        }
      }
    }
    return usage;
  }

  @Override
  public void updateDemand() {
    // Compute demand by iterating through apps in the queue
    // Limit demand to maxResources
    Map<String, Resource> maxRes = scheduler.getAllocationConfiguration()
        .getMaxResources(getName());
    demand = new HashMap<String, Resource>();
    for (FSQueue childQueue : childQueues) {
      childQueue.updateDemand();
      Map<String, Resource> toAdd = childQueue.getDemand();
      for (Map.Entry<String, Resource> use : toAdd.entrySet()) {
        if (!demand.containsKey(use.getKey())) {
          demand.put(use.getKey(), Resources.clone(use.getValue()));
        } else {
          demand.put(use.getKey(), Resources.addTo(demand.get(use.getKey()),
              use.getValue()));
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Parent queue<" + getName() + "> upgrade demand for<" + use.getValue()
              + "> from childQueue<" + childQueue.getName() + "> on node label<"
              + use.getKey() == "" ? "NO_LABEL" : use.getKey()
              + ">.");
        }
      }
    }

    //Min(demand, MaxShare)
    for (Map.Entry<String, Resource> entry : demand.entrySet()) {
      if (!maxRes.containsKey(entry.getKey())) {
        entry.setValue(Resources.componentwiseMin(
            entry.getValue(), Resources.unbounded()));
      } else {
        entry.setValue(Resources.componentwiseMin(entry.getValue(),
            maxRes.get(entry.getKey())));
      }
    }

    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, Resource> entry : demand.entrySet()) {
        LOG.debug("The updated demand for " + getName() + " is "
            + entry.getValue() +
            "; the max is "
            + maxRes.get(entry.getKey()) == null
              ? Resources.unbounded()
              : maxRes.get(entry.getKey())
            + " on node label: "
            + entry.getKey() == ""
              ? "NO_LABEL"
              : entry.getKey());
      }
    }
  }
  
  private synchronized QueueUserACLInfo getUserAclInfo(
      UserGroupInformation user) {
    QueueUserACLInfo userAclInfo = 
      recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<QueueACL>();
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      } 
    }

    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return userAclInfo;
  }
  
  @Override
  public synchronized List<QueueUserACLInfo> getQueueUserAclInfo(
      UserGroupInformation user) {
    List<QueueUserACLInfo> userAcls = new ArrayList<QueueUserACLInfo>();
    
    // Add queue acls
    userAcls.add(getUserAclInfo(user));
    
    // Add children queue acls
    for (FSQueue child : childQueues) {
      userAcls.addAll(child.getQueueUserAclInfo(user));
    }
 
    return userAcls;
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    Resource assigned = Resources.none();

    // If this queue is over its limit, reject
    if (!assignContainerPreCheck(node)) {
      return assigned;
    }

    Collections.sort(childQueues, policy.getComparator());
    for (FSQueue child : childQueues) {
      assigned = child.assignContainer(node);
      if (!Resources.equals(assigned, Resources.none())) {
        break;
      }
    }
    return assigned;
  }

  @Override
  public Resource assignGPUContainer(FSSchedulerNode node) {
    Resource assigned = Resources.none();

    // If this queue is over its limit, reject
    //TODO REGARD ResourceType(GPU...) WHEN CHECK THIS
    if (!assignContainerPreCheck(node)) {
      return assigned;
    }
    Collections.sort(childQueues, SchedulingPolicy.GPU_POLICY.getComparator());
    LOG.error("Nodeheartbeat:::");
    for (FSQueue child : childQueues) {
      LOG.error("Assgin Queue: " + child.getName() + " gpu usage: " + child
          .getResourceUsage()
          .get(node.getLabels()
          .iterator().next())
          .getGpuCores());
      assigned = child.assignGPUContainer(node);
      if (!Resources.equals(assigned, Resources.none())) {
        break;
      }
    }
    return assigned;
  }

  @Override
  public Set<RMContainer> preemptContainer() {
    RMContainer toBePreempted = null;

    // Find the childQueue which is most over fair share
    FSQueue candidateQueue = null;
    Comparator<Schedulable> comparator = policy.getComparator();
    for (FSQueue queue : childQueues) {
      if (candidateQueue == null ||
          comparator.compare(queue, candidateQueue) > 0) {
        candidateQueue = queue;
      }
    }

    // Let the selected queue choose which of its container to preempt
    if (candidateQueue != null) {
      toBePreempted = candidateQueue.preemptContainer();
    }
    return toBePreempted;
  }

  @Override
  public List<FSQueue> getChildQueues() {
    return childQueues;
  }

  @Override
  public void setPolicy(SchedulingPolicy policy)
      throws AllocationConfigurationException {
    boolean allowed =
        SchedulingPolicy.isApplicableTo(policy, (parent == null)
            ? SchedulingPolicy.DEPTH_ROOT
            : SchedulingPolicy.DEPTH_INTERMEDIATE);
    if (!allowed) {
      throwPolicyDoesnotApplyException(policy);
    }
    super.policy = policy;
  }
  
  public void incrementRunnableApps() {
    runnableApps++;
  }
  
  public void decrementRunnableApps() {
    runnableApps--;
  }

  @Override
  public int getNumRunnableApps() {
    return runnableApps;
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    for (FSQueue childQueue : childQueues) {
      childQueue.collectSchedulerApplications(apps);
    }
  }
  
  @Override
  public ActiveUsersManager getActiveUsersManager() {
    // Should never be called since all applications are submitted to LeafQueues
    return null;
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
    // TODO Auto-generated method stub
    
  }
}
