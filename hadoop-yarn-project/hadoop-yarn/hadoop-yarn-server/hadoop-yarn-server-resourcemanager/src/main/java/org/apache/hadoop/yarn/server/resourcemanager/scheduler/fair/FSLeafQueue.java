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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.sun.tools.corba.se.idl.StringGen;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Unstable
public class FSLeafQueue extends FSQueue {
  private static final Log LOG = LogFactory.getLog(
      FSLeafQueue.class.getName());

  private final List<FSAppAttempt> runnableApps = // apps that are runnable
      new ArrayList<FSAppAttempt>();
  private final List<FSAppAttempt> nonRunnableApps =
      new ArrayList<FSAppAttempt>();
  // get a lock with fair distribution for app list updates
  private final ReadWriteLock rwl = new ReentrantReadWriteLock(true);
  private final Lock readLock = rwl.readLock();
  private final Lock writeLock = rwl.writeLock();
  
  private Map<String, Resource> demand = Resources.createComposeResource();
  
  // Variables used for preemption
  private Map<String, Long> lastTimeAtMinShare;
  private Map<String, Long> lastTimeAtFairShareThreshold;
  
  // Track the AM resource usage for this queue
  private Resource amResourceUsage;

  private final ActiveUsersManager activeUsersManager;
  
  public FSLeafQueue(String name, FairScheduler scheduler,
      FSParentQueue parent) {
    super(name, scheduler, parent);

    this.lastTimeAtMinShare = new HashMap<String, Long>();
    this.lastTimeAtFairShareThreshold = new HashMap<String, Long>();
    //initilize time
    for (String nodeLabel : getAccessibleNodeLabels()) {
      lastTimeAtMinShare.put(nodeLabel, scheduler.getClock().getTime());
      lastTimeAtFairShareThreshold.put(nodeLabel, scheduler.getClock().getTime());
    }

    activeUsersManager = new ActiveUsersManager(getMetrics());
    amResourceUsage = Resource.newInstance(0, 0, 0);
  }
  
  public void addApp(FSAppAttempt app, boolean runnable) {
    writeLock.lock();
    try {
      if (runnable) {
        runnableApps.add(app);
      } else {
        nonRunnableApps.add(app);
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  // for testing
  void addAppSchedulable(FSAppAttempt appSched) {
    writeLock.lock();
    try {
      runnableApps.add(appSched);
    } finally {
      writeLock.unlock();
    }
  }
  
  /**
   * Removes the given app from this queue.
   * @return whether or not the app was runnable
   */
  public boolean removeApp(FSAppAttempt app) {
    boolean runnable = false;

    // Remove app from runnable/nonRunnable list while holding the write lock
    writeLock.lock();
    try {
      runnable = runnableApps.remove(app);
      if (!runnable) {
        // removeNonRunnableApp acquires the write lock again, which is fine
        if (!removeNonRunnableApp(app)) {
          throw new IllegalStateException("Given app to remove " + app +
              " does not exist in queue " + this);
        }
      }
    } finally {
      writeLock.unlock();
    }

    // Update AM resource usage if needed
    if (runnable && app.isAmRunning() && app.getAMResource() != null) {
      Resources.subtractFrom(amResourceUsage, app.getAMResource());
    }

    return runnable;
  }

  /**
   * Removes the given app if it is non-runnable and belongs to this queue
   * @return true if the app is removed, false otherwise
   */
  public boolean removeNonRunnableApp(FSAppAttempt app) {
    writeLock.lock();
    try {
      return nonRunnableApps.remove(app);
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isRunnableApp(FSAppAttempt attempt) {
    readLock.lock();
    try {
      return runnableApps.contains(attempt);
    } finally {
      readLock.unlock();
    }
  }

  public boolean isNonRunnableApp(FSAppAttempt attempt) {
    readLock.lock();
    try {
      return nonRunnableApps.contains(attempt);
    } finally {
      readLock.unlock();
    }
  }

  public void resetPreemptedResources() {
    readLock.lock();
    try {
      for (FSAppAttempt attempt : runnableApps) {
        attempt.resetPreemptedResources();
      }
    } finally {
      readLock.unlock();
    }
  }

  public void clearPreemptedResources() {
    readLock.lock();
    try {
      for (FSAppAttempt attempt : runnableApps) {
        attempt.clearPreemptedResources();
      }
    } finally {
      readLock.unlock();
    }
  }

  public List<FSAppAttempt> getCopyOfNonRunnableAppSchedulables() {
    List<FSAppAttempt> appsToReturn = new ArrayList<FSAppAttempt>();
    readLock.lock();
    try {
      appsToReturn.addAll(nonRunnableApps);
    } finally {
      readLock.unlock();
    }
    return appsToReturn;
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    readLock.lock();
    try {
      for (FSAppAttempt appSched : runnableApps) {
        apps.add(appSched.getApplicationAttemptId());
      }
      for (FSAppAttempt appSched : nonRunnableApps) {
        apps.add(appSched.getApplicationAttemptId());
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void setPolicy(SchedulingPolicy policy)
      throws AllocationConfigurationException {
    if (!SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_LEAF)) {
      throwPolicyDoesnotApplyException(policy);
    }
    super.policy = policy;
  }
  
  @Override
  public void recomputeShares() {
    readLock.lock();
    try {
      policy.computeShares(runnableApps, getFairShare());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Map<String, Resource> getDemand() {
    return demand;
  }

  @Override
  public Map<String, Resource> getResourceUsage() {
    Map<String, Resource> usage = Resources.createComposeResource();
    readLock.lock();
    try {
      for (FSAppAttempt app : runnableApps) {
        Map<String, Resource> appUse = app.getResourceUsage();
        for (String nodeLabel : appUse.keySet()) {
          if(usage.containsKey(nodeLabel)) {
            Resources.addTo(usage.get(nodeLabel), appUse.get(nodeLabel));
          } else {
            usage.put(nodeLabel, Resources.clone(appUse.get(nodeLabel)));
          }
        }
      }
      for (FSAppAttempt app : nonRunnableApps) {
        Map<String, Resource> appUse = app.getResourceUsage();
        for (String nodeLabel : appUse.keySet()) {
          if(usage.containsKey(nodeLabel)) {
            Resources.addTo(usage.get(nodeLabel), appUse.get(nodeLabel));
          } else {
            usage.put(nodeLabel, Resources.clone(appUse.get(nodeLabel)));
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return usage;
  }

  public Resource getAmResourceUsage() {
    return amResourceUsage;
  }

  @Override
  public void updateDemand() {
    // Compute demand by iterating through apps in the queue
    // Limit demand to maxResources
    Map<String, Resource> maxRes = scheduler.getAllocationConfiguration()
        .getMaxResources(getName());
    demand = Resources.createComposeResource();
    readLock.lock();
    try {
      for (FSAppAttempt sched : runnableApps) {
        updateDemandForApp(sched, maxRes);
      }
      for (FSAppAttempt sched : nonRunnableApps) {
        updateDemandForApp(sched, maxRes);
      }
    } finally {
      readLock.unlock();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("The updated demand for " + getName() + " is " + demand
          + "; the max is " + maxRes);
    }
  }
  
  private void updateDemandForApp(FSAppAttempt sched, Map<String, Resource> maxRes) {
    sched.updateDemand();
    Map<String, Resource> toAdd = sched.getDemand();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Counting resource from " + sched.getName() + " " + toAdd
          + "; Total resource consumption for " + getName() + " now "
          + demand);
    }

    for(String nodeLable : toAdd.keySet()) {
      if(demand.containsKey(nodeLable)) {
        Resources.addTo(demand.get(nodeLable), toAdd.get(nodeLable));
      } else {
        demand.put(nodeLable, Resources.clone(toAdd.get(nodeLable)));
      }
      demand.put(nodeLable, Resources.componentwiseMin(
          demand.get(nodeLable), maxRes.get(nodeLable)));
    }
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    Resource assigned = Resources.none();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node " + node.getNodeName() + " offered to queue: " +
          getName());
    }

    if (!assignContainerPreCheck(node)) {
      return assigned;
    }

    final MyComparator<Schedulable, String> comparator = policy.getComparator();
    final String nodeLabel = node.getLabels().iterator().next();
    writeLock.lock();
    try {
      Collections.sort(runnableApps, new Comparator<FSAppAttempt>() {
        @Override
        public int compare(FSAppAttempt o1, FSAppAttempt o2) {
          return comparator.compare(o1, o2, nodeLabel);
        }
      });
    } finally {
      writeLock.unlock();
    }
    // Release write lock here for better performance and avoiding deadlocks.
    // runnableApps can be in unsorted state because of this section,
    // but we can accept it in practice since the probability is low.
    readLock.lock();
    try {
      for (FSAppAttempt sched : runnableApps) {
        if (SchedulerAppUtils.isBlacklisted(sched, node, LOG)) {
          continue;
        }

        assigned = sched.assignContainer(node);
        if (!assigned.equals(Resources.none())) {
          break;
        }
      }
    } finally {
      readLock.unlock();
    }
    return assigned;
  }

  @Override
  public Resource assignGPUContainer(FSSchedulerNode node) {
    Resource assigned = Resources.none();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node " + node.getNodeName() + " offered to queue: " +
          getName());
    }

    if (!assignContainerPreCheck(node)) {
      return assigned;
    }

    final MyComparator<Schedulable, String> comparator = SchedulingPolicy.FIFO_POLICY.getComparator();
    final String nodeLabel = node.getLabels().iterator().next();
    writeLock.lock();
    try {
      Collections.sort(runnableApps, new Comparator<FSAppAttempt>() {
        @Override
        public int compare(FSAppAttempt o1, FSAppAttempt o2) {
          return comparator.compare(o1, o2, nodeLabel);
        }
      });
    } finally {
      writeLock.unlock();
    }
    // Release write lock here for better performance and avoiding deadlocks.
    // runnableApps can be in unsorted state because of this section,
    // but we can accept it in practice since the probability is low.
    readLock.lock();
    for (FSAppAttempt sched : runnableApps){
      LOG.info("App order: " + sched.getName() + " with priorty: " + sched.getPriority() + " startTime: " +
            sched.getStartTime());
    }
    try {
      for (FSAppAttempt sched : runnableApps) {
        if (SchedulerAppUtils.isBlacklisted(sched, node, LOG)) {
          continue;
        }

        LOG.info("Try to assign app: " + sched);
        assigned = sched.assignContainer(node);
        if (!assigned.equals(Resources.none())) {
          break;
        }
      }
    } finally {
      readLock.unlock();
    }
    return assigned;
  }

  @Override
  public RMContainer preemptContainer(String nodeLabel) {
    RMContainer toBePreempted = null;

    // If this queue is not over its fair share, reject
    if (!preemptContainerPreCheck(nodeLabel))
      return toBePreempted;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Queue " + getName() + " is going to preempt a container " +
          "from its applications.");
    }

    // Choose the app that is most over fair share
    MyComparator<Schedulable, String> comparator = policy.getComparator();
    if (demand.get(nodeLabel).getGpuCores() > 0) {
      // Use gpu comparator.
      comparator = SchedulingPolicy.GPU_POLICY.getComparator();
    }

    FSAppAttempt candidateSched = null;
    readLock.lock();
    try {
      for (FSAppAttempt sched : runnableApps) {
        if (candidateSched == null ||
            comparator.compare(sched, candidateSched, nodeLabel) > 0) {
          candidateSched = sched;
        }
      }
    } finally {
      readLock.unlock();
    }

    // Preempt from the selected app
    if (candidateSched != null) {
      toBePreempted = candidateSched.preemptContainer(nodeLabel);
    }

    return toBePreempted;
  }

  @Override
  public List<FSQueue> getChildQueues() {
    return new ArrayList<FSQueue>(1);
  }
  
  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
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
    return Collections.singletonList(userAclInfo);
  }
  
  public Map<String, Long> getLastTimeAtMinShare() {
    return lastTimeAtMinShare;
  }

  private void setLastTimeAtMinShare(String nodeLabel) {
    long now = scheduler.getClock().getTime();
    this.lastTimeAtMinShare.put(nodeLabel, now);
  }

  public Map<String, Long> getLastTimeAtFairShareThreshold() {
    return lastTimeAtFairShareThreshold;
  }

  private void setLastTimeAtFairShareThreshold(
      String nodeLabel) {
    long now = scheduler.getClock().getTime();
    this.lastTimeAtFairShareThreshold.put(nodeLabel, now);
  }

  @Override
  public int getNumRunnableApps() {
    readLock.lock();
    try {
      return runnableApps.size();
    } finally {
      readLock.unlock();
    }
  }

  public int getNumNonRunnableApps() {
    readLock.lock();
    try {
      return nonRunnableApps.size();
    } finally {
      readLock.unlock();
    }
  }

  public int getNumPendingApps() {
    int numPendingApps = 0;
    readLock.lock();
    try {
      for (FSAppAttempt attempt : runnableApps) {
        if (attempt.isPending()) {
          numPendingApps++;
        }
      }
      numPendingApps += nonRunnableApps.size();
    } finally {
      readLock.unlock();
    }
    return numPendingApps;
  }

  /**
   * TODO: Based on how frequently this is called, we might want to club
   * counting pending and active apps in the same method.
   */
  public int getNumActiveApps() {
    int numActiveApps = 0;
    readLock.lock();
    try {
      for (FSAppAttempt attempt : runnableApps) {
        if (!attempt.isPending()) {
          numActiveApps++;
        }
      }
    } finally {
      readLock.unlock();
    }
    return numActiveApps;
  }

  @Override
  public ActiveUsersManager getActiveUsersManager() {
    return activeUsersManager;
  }

  /**
   * Check whether this queue can run this application master under the
   * maxAMShare limit
   *
   * @param amResource
   * @return true if this queue can run
   */
  public boolean canRunAppAM(Resource amResource) {
    float maxAMShare =
        scheduler.getAllocationConfiguration().getQueueMaxAMShare(getName());
    if (Math.abs(maxAMShare - -1.0f) < 0.0001) {
      return true;
    }
    // Step one just consider NO_LABEL
    // TODO MAKE MORE SANITY
    Resource maxAMResource = Resources.multiply(getFairShare().get(""), maxAMShare);
    Resource ifRunAMResource = Resources.add(amResourceUsage, amResource);
    return !policy
        .checkIfAMResourceUsageOverLimit(ifRunAMResource, maxAMResource);
  }

  public void addAMResourceUsage(Resource amResource) {
    if (amResource != null) {
      Resources.addTo(amResourceUsage, amResource);
    }
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
    // TODO Auto-generated method stub
  }

  /**
   * Update the preemption fields for the queue, i.e. the times since last was
   * at its guaranteed share and over its fair share threshold.
   */
  public void updateStarvationStats() {
    for (String nodeLabel : getAccessibleNodeLabels()) {
      if (!isStarvedForMinShare(nodeLabel)) {
        setLastTimeAtMinShare(nodeLabel);
      }
      if (!isStarvedForFairShare(nodeLabel)) {
        setLastTimeAtFairShareThreshold(nodeLabel);
      }
    }
  }

  /** Allows setting weight for a dynamically created queue
   * Currently only used for reservation based queues
   * @param weight queue weight
   */
  public void setWeights(float weight) {
    scheduler.getAllocationConfiguration().setQueueWeight(getName(),
        new ResourceWeights(weight));
  }

  /**
   * Helper method to check if the queue should preempt containers
   *
   * @return true if check passes (can preempt) or false otherwise
   */
  private boolean preemptContainerPreCheck(String nodeLabel) {
    return parent.getPolicy().checkIfUsageOverFairShare(getResourceUsage().get(nodeLabel),
        getFairShare().get(nodeLabel));
  }

  /**
   * Is a queue being starved for its min share.
   */
  @VisibleForTesting
  boolean isStarvedForMinShare(String nodeLabel) {
    return isStarved(nodeLabel);
  }

  /**
   * Is a queue being starved for its fair share threshold.
   */
  @VisibleForTesting
  boolean isStarvedForFairShare(String nodeLabel) {
    Resource fairShare = Resources.min(scheduler.getResourceCalculator(),
        scheduler.getClusterResource().get(nodeLabel), Resources.multiply(getFairShare().get(nodeLabel),
            getFairSharePreemptionThreshold()), getDemand().get(nodeLabel));
    return Resources.lessThan(scheduler.getResourceCalculator(),
        scheduler.getClusterResource().get(nodeLabel), getResourceUsage().get(nodeLabel),
        fairShare
        );
  }

  private boolean isStarved(String nodeLabel) {
    Resource minShare = getMinShare().get(nodeLabel);
    Resource desiredShare = Resources.min(scheduler.getResourceCalculator(),
        scheduler.getClusterResource().get(nodeLabel), minShare, getDemand().get(nodeLabel));
    return Resources.lessThan(scheduler.getResourceCalculator(),
        scheduler.getClusterResource().get(nodeLabel), getResourceUsage().get(nodeLabel), desiredShare);
  }
}
