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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.*;
import org.apache.hadoop.tools.metrics.NoNullHashMap;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

@InterfaceAudience.Private
@Metrics(context="yarn")
public class QueueMetrics implements MetricsSource {
  @Metric("# of apps submitted") MutableCounterInt appsSubmitted;
  @Metric("# of running apps") MutableGaugeInt appsRunning;
  @Metric("# of pending apps") MutableGaugeInt appsPending;
  @Metric("# of apps completed") MutableCounterInt appsCompleted;
  @Metric("# of apps killed") MutableCounterInt appsKilled;
  @Metric("# of apps failed") MutableCounterInt appsFailed;

  @Metric("Allocated memory in MB") MutableMapGaugeInt allocatedMB;
  @Metric("Allocated CPU in virtual cores") MutableMapGaugeInt allocatedVCores;
  @Metric("Allocated GPU in cores") MutableMapGaugeInt allocatedGCores;
  @Metric("# of allocated containers") MutableMapGaugeInt allocatedContainers;
  @Metric("Aggregate # of allocated containers") MutableCounterLong aggregateContainersAllocated;
  @Metric("Aggregate # of allocated node-local containers")
    MutableCounterLong aggregateNodeLocalContainersAllocated;
  @Metric("Aggregate # of allocated rack-local containers")
    MutableCounterLong aggregateRackLocalContainersAllocated;
  @Metric("Aggregate # of allocated off-switch containers")
    MutableCounterLong aggregateOffSwitchContainersAllocated;
  @Metric("Aggregate # of released containers") MutableCounterLong aggregateContainersReleased;
  @Metric("Available memory in MB") MutableMapGaugeInt availableMB;
  @Metric("Available CPU in virtual cores") MutableMapGaugeInt availableVCores;
  @Metric("Available GPU in cores") MutableMapGaugeInt availableGCores;
  @Metric("Pending memory allocation in MB") MutableMapGaugeInt pendingMB;
  @Metric("Pending CPU allocation in virtual cores") MutableMapGaugeInt pendingVCores;
  @Metric("Pending GPU allocation in cores") MutableMapGaugeInt pendingGCores;
  @Metric("# of pending containers") MutableMapGaugeInt pendingContainers;
  @Metric("# of reserved memory in MB") MutableMapGaugeInt reservedMB;
  @Metric("Reserved CPU in virtual cores") MutableMapGaugeInt reservedVCores;
  @Metric("Reserved GPU in cores") MutableMapGaugeInt reservedGCores;
  @Metric("# of reserved containers") MutableMapGaugeInt reservedContainers;
  @Metric("# of active users") MutableGaugeInt activeUsers;
  @Metric("# of active applications") MutableGaugeInt activeApplications;
  private final MutableGaugeInt[] runningTime;
  private TimeBucketMetrics<ApplicationId> runBuckets;

  static final Logger LOG = LoggerFactory.getLogger(QueueMetrics.class);
  static final MetricsInfo RECORD_INFO = info("QueueMetrics",
      "Metrics for the resource scheduler");
  public static final MetricsInfo QUEUE_INFO = info("Queue", "Metrics by queue");
  public static final MetricsInfo USER_INFO = info("User", "Metrics by user");
  public  static final Splitter Q_SPLITTER =
      Splitter.on('.').omitEmptyStrings().trimResults();

  final MetricsRegistry registry;
  final String queueName;
  final QueueMetrics parent;
  final MetricsSystem metricsSystem;
  private final Map<String, QueueMetrics> users;
  private final Configuration conf;

  public QueueMetrics() {
    runningTime = new MutableGaugeInt[1];
    registry = new MetricsRegistry(RECORD_INFO);
    queueName = "root";
    parent = null;
    users = null;
    metricsSystem = null;
    conf = new Configuration();
  }

  protected QueueMetrics(MetricsSystem ms, String queueName, Queue parent, 
	       boolean enableUserMetrics, Configuration conf) {
    registry = new MetricsRegistry(RECORD_INFO);
    this.queueName = queueName;
    this.parent = parent != null ? parent.getMetrics() : null;
    this.users = enableUserMetrics ? new HashMap<String, QueueMetrics>()
                                   : null;
    metricsSystem = ms;
    this.conf = conf;
    runningTime = buildBuckets(conf);
  }

  protected QueueMetrics tag(MetricsInfo info, String value) {
    registry.tag(info, value);
    return this;
  }

  protected static StringBuilder sourceName(String queueName) {
    StringBuilder sb = new StringBuilder(RECORD_INFO.name());
    int i = 0;
    for (String node : Q_SPLITTER.split(queueName)) {
      sb.append(",q").append(i++).append('=').append(node);
    }
    return sb;
  }

  public synchronized
  static QueueMetrics forQueue(String queueName, Queue parent,
                               boolean enableUserMetrics,
			       Configuration conf) {
    return forQueue(DefaultMetricsSystem.instance(), queueName, parent,
                    enableUserMetrics, conf);
  }

  /**
   * Helper method to clear cache.
   */
  @Private
  public synchronized static void clearQueueMetrics() {
    queueMetrics.clear();
  }
  
  /**
   * Simple metrics cache to help prevent re-registrations.
   */
  protected final static Map<String, QueueMetrics> queueMetrics =
      new HashMap<String, QueueMetrics>();
  
  public synchronized 
  static QueueMetrics forQueue(MetricsSystem ms, String queueName,
                                      Queue parent, boolean enableUserMetrics,
				      Configuration conf) {
    QueueMetrics metrics = queueMetrics.get(queueName);
    if (metrics == null) {
      metrics =
          new QueueMetrics(ms, queueName, parent, enableUserMetrics, conf).
          tag(QUEUE_INFO, queueName);
      
      // Register with the MetricsSystems
      if (ms != null) {
        metrics = 
            ms.register(
                sourceName(queueName).toString(), 
                "Metrics for queue: " + queueName, metrics);
      }
      queueMetrics.put(queueName, metrics);
    }

    return metrics;
  }

  public synchronized QueueMetrics getUserMetrics(String userName) {
    if (users == null) {
      return null;
    }
    QueueMetrics metrics = users.get(userName);
    if (metrics == null) {
      metrics = new QueueMetrics(metricsSystem, queueName, null, false, conf);
      users.put(userName, metrics);
      metricsSystem.register(
          sourceName(queueName).append(",user=").append(userName).toString(),
          "Metrics for user '"+ userName +"' in queue '"+ queueName +"'",
          metrics.tag(QUEUE_INFO, queueName).tag(USER_INFO, userName));
    }
    return metrics;
  }

  private ArrayList<Integer> parseInts(String value) {
    ArrayList<Integer> result = new ArrayList<Integer>();
    for(String s: value.split(",")) {
      result.add(Integer.parseInt(s.trim()));
    }
    return result;
  }

  private MutableGaugeInt[] buildBuckets(Configuration conf) {
    ArrayList<Integer> buckets = 
      parseInts(conf.get(YarnConfiguration.RM_METRICS_RUNTIME_BUCKETS,
		        YarnConfiguration.DEFAULT_RM_METRICS_RUNTIME_BUCKETS));
    MutableGaugeInt[] result = new MutableGaugeInt[buckets.size() + 1];
    result[0] = registry.newGauge("running_0", "", 0);
    long[] cuts = new long[buckets.size()];
    for(int i=0; i < buckets.size(); ++i) {
      result[i+1] = registry.newGauge("running_" + buckets.get(i), "", 0);
      cuts[i] = buckets.get(i) * 1000L * 60; // covert from min to ms
    }
    this.runBuckets = new TimeBucketMetrics<ApplicationId>(cuts);
    return result;
  }

  private void updateRunningTime() {
    int[] counts = runBuckets.getBucketCounts(System.currentTimeMillis());
    for(int i=0; i < counts.length; ++i) {
      runningTime[i].set(counts[i]); 
    }
  }

  public void getMetrics(MetricsCollector collector, boolean all) {
    updateRunningTime();
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

  public void submitApp(String user) {
    appsSubmitted.incr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.submitApp(user);
    }
    if (parent != null) {
      parent.submitApp(user);
    }
  }

  public void submitAppAttempt(String user) {
    appsPending.incr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.submitAppAttempt(user);
    }
    if (parent != null) {
      parent.submitAppAttempt(user);
    }
  }

  public void runAppAttempt(ApplicationId appId, String user) {
    runBuckets.add(appId, System.currentTimeMillis());
    appsRunning.incr();
    appsPending.decr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.runAppAttempt(appId, user);
    }
    if (parent != null) {
      parent.runAppAttempt(appId, user);
    }
  }

  public void finishAppAttempt(
      ApplicationId appId, boolean isPending, String user) {
    runBuckets.remove(appId);
    if (isPending) {
      appsPending.decr();
    } else {
      appsRunning.decr();
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.finishAppAttempt(appId, isPending, user);
    }
    if (parent != null) {
      parent.finishAppAttempt(appId, isPending, user);
    }
  }

  public void finishApp(String user, RMAppState rmAppFinalState) {
    switch (rmAppFinalState) {
      case KILLED: appsKilled.incr(); break;
      case FAILED: appsFailed.incr(); break;
      default: appsCompleted.incr();  break;
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.finishApp(user, rmAppFinalState);
    }
    if (parent != null) {
      parent.finishApp(user, rmAppFinalState);
    }
  }
  
  public void moveAppFrom(AppSchedulingInfo app) {
    if (app.isPending()) {
      appsPending.decr();
    } else {
      appsRunning.decr();
    }
    QueueMetrics userMetrics = getUserMetrics(app.getUser());
    if (userMetrics != null) {
      userMetrics.moveAppFrom(app);
    }
    if (parent != null) {
      parent.moveAppFrom(app);
    }
  }
  
  public void moveAppTo(AppSchedulingInfo app) {
    if (app.isPending()) {
      appsPending.incr();
    } else {
      appsRunning.incr();
    }
    QueueMetrics userMetrics = getUserMetrics(app.getUser());
    if (userMetrics != null) {
      userMetrics.moveAppTo(app);
    }
    if (parent != null) {
      parent.moveAppTo(app);
    }
  }

  /**
   * Set available resources. To be called by scheduler periodically as
   * resources become available.
   * @param clusterResource resource limit
   */
  public void setAvailableResourcesToQueue(Map<String, Resource> clusterResource) {
    for (String nodeLabel : clusterResource.keySet()) {
      Resource limit = Resources.subtract(
          clusterResource.get(nodeLabel), getAllocatedResources().get(nodeLabel));
      availableMB.set(nodeLabel, limit.getMemory());
      availableVCores.set(nodeLabel, limit.getVirtualCores());
      availableGCores.set(nodeLabel, limit.getGpuCores());
    }
  }

  /**
   * Set available resources. To be called by scheduler periodically as
   * resources become available.
   * @param user
   * @param limit resource limit
   */
  public void setAvailableResourcesToUser(String user, Resource limit) {
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.setAvailableResourcesToQueue(Resources.createComposeResource());
    }
  }

  /**
   * Increment pending resource metrics
   * @param user
   * @param containers
   * @param res the TOTAL delta of resources note this is different from
   *            the other APIs which use per container resource
   */
  public void incrPendingResources(String user, int containers, Resource res, String nodeLabel) {
    _incrPendingResources(containers, res, nodeLabel);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.incrPendingResources(user, containers, res, nodeLabel);
    }
    if (parent != null) {
      parent.incrPendingResources(user, containers, res, nodeLabel);
    }
  }

  private void _incrPendingResources(int containers, Resource res, String nodeLabel) {
    pendingContainers.incr(nodeLabel, containers);
    pendingMB.incr(nodeLabel, res.getMemory() * containers);
    pendingVCores.incr(nodeLabel, res.getVirtualCores() * containers);
    pendingGCores.incr(nodeLabel, res.getGpuCores() * containers);
  }

  public void decrPendingResources(String user, int containers, Resource res, String nodeLabel) {
    _decrPendingResources(containers, res, nodeLabel);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.decrPendingResources(user, containers, res, nodeLabel);
    }
    if (parent != null) {
      parent.decrPendingResources(user, containers, res, nodeLabel);
    }
  }

  private void _decrPendingResources(int containers, Resource res, String nodeLabel) {
    pendingContainers.decr(nodeLabel, containers);
    pendingMB.decr(nodeLabel, res.getMemory() * containers);
    pendingVCores.decr(nodeLabel, res.getVirtualCores() * containers);
    pendingGCores.decr(nodeLabel, res.getGpuCores() * containers);
  }

  public void incrNodeTypeAggregations(String user, NodeType type) {
    if (type == NodeType.NODE_LOCAL) {
      aggregateNodeLocalContainersAllocated.incr();
    } else if (type == NodeType.RACK_LOCAL) {
      aggregateRackLocalContainersAllocated.incr();
    } else if (type == NodeType.OFF_SWITCH) {
      aggregateOffSwitchContainersAllocated.incr();
    } else {
      return;
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.incrNodeTypeAggregations(user, type);
    }
    if (parent != null) {
      parent.incrNodeTypeAggregations(user, type);
    }
  }

  public void allocateResources(String user, int containers, Resource res,
      boolean decrPending, String nodeLabel) {
    allocatedContainers.incr(nodeLabel, containers);
    aggregateContainersAllocated.incr(containers);
    allocatedMB.incr(nodeLabel, res.getMemory() * containers);
    allocatedVCores.incr(nodeLabel, res.getVirtualCores() * containers);
    allocatedGCores.incr(nodeLabel, res.getGpuCores() * containers);
    if (decrPending) {
      _decrPendingResources(containers, res, nodeLabel);
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.allocateResources(user, containers, res, decrPending, nodeLabel);
    }
    if (parent != null) {
      parent.allocateResources(user, containers, res, decrPending, nodeLabel);
    }
  }

  public void releaseResources(String user, int containers, Resource res, String nodeLabel) {
    allocatedContainers.decr(nodeLabel, containers);
    aggregateContainersReleased.incr(containers);
    allocatedMB.decr(nodeLabel, res.getMemory() * containers);
    allocatedVCores.decr(nodeLabel, res.getVirtualCores() * containers);
    allocatedGCores.decr(nodeLabel, res.getGpuCores() * containers);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.releaseResources(user, containers, res, nodeLabel);
    }
    if (parent != null) {
      parent.releaseResources(user, containers, res, nodeLabel);
    }
  }

  public void reserveResource(String user, Resource res, String nodeLabel) {
    reservedContainers.incr(nodeLabel);
    reservedMB.incr(nodeLabel, res.getMemory());
    reservedVCores.incr(nodeLabel, res.getVirtualCores());
    reservedGCores.incr(nodeLabel, res.getGpuCores());
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.reserveResource(user, res, nodeLabel);
    }
    if (parent != null) {
      parent.reserveResource(user, res, nodeLabel);
    }
  }

  public void unreserveResource(String user, Resource res, String nodeLabel) {
    reservedContainers.decr(nodeLabel);
    reservedMB.decr(nodeLabel, res.getMemory());
    reservedVCores.decr(nodeLabel, res.getVirtualCores());
    reservedGCores.decr(nodeLabel, res.getGpuCores());
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.unreserveResource(user, res, nodeLabel);
    }
    if (parent != null) {
      parent.unreserveResource(user, res, nodeLabel);
    }
  }

  public void incrActiveUsers() {
    activeUsers.incr();
  }
  
  public void decrActiveUsers() {
    activeUsers.decr();
  }
  
  public void activateApp(String user) {
    activeApplications.incr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.activateApp(user);
    }
    if (parent != null) {
      parent.activateApp(user);
    }
  }
  
  public void deactivateApp(String user) {
    activeApplications.decr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.deactivateApp(user);
    }
    if (parent != null) {
      parent.deactivateApp(user);
    }
  }
  
  public int getAppsSubmitted() {
    return appsSubmitted.value();
  }

  public int getAppsRunning() {
    return appsRunning.value();
  }

  public int getAppsPending() {
    return appsPending.value();
  }

  public int getAppsCompleted() {
    return appsCompleted.value();
  }

  public int getAppsKilled() {
    return appsKilled.value();
  }

  public int getAppsFailed() {
    return appsFailed.value();
  }
  
  public Map<String, Resource> getAllocatedResources() {
    Map<String, Resource> allocated = new HashMap<String, Resource>();
    for (String nodeLabel : allocatedContainers.getValue().keySet()) {
      allocated.put(nodeLabel,
          BuilderUtils.newResource(
              allocatedMB.value(nodeLabel),
              allocatedVCores.value(nodeLabel),
              allocatedGCores.value(nodeLabel)));
    }
      return allocated;
  }

  public MutableMapGaugeInt getAllocatedMB() {
    return allocatedMB;
  }
  
  public MutableMapGaugeInt getAllocatedVirtualCores() {
    return allocatedVCores;
  }

  public MutableMapGaugeInt getAllocatedGpuCores() {
    return allocatedGCores;
  }

  public MutableMapGaugeInt getAllocatedContainers() {
    return allocatedContainers;
  }

  public MutableMapGaugeInt getAvailableMB() {
    return availableMB;
  }  
  
  public MutableMapGaugeInt getAvailableVirtualCores() {
    return availableVCores;
  }

  public MutableMapGaugeInt getAvailableGpuCores() {
    return availableGCores;
  }

  public MutableMapGaugeInt getPendingMB() {
    return pendingMB;
  }
  
  public MutableMapGaugeInt getPendingVirtualCores() {
    return pendingVCores;
  }

  public MutableMapGaugeInt getPendingGpuCores() {
    return pendingGCores;
  }

  public MutableMapGaugeInt getPendingContainers() {
    return pendingContainers;
  }
  
  public MutableMapGaugeInt getReservedMB() {
    return reservedMB;
  }
  
  public MutableMapGaugeInt getReservedVirtualCores() {
    return reservedVCores;
  }

  public MutableMapGaugeInt getReservedGpuCores() {
    return reservedGCores;
  }

  public MutableMapGaugeInt getReservedContainers() {
    return reservedContainers;
  }
  
  public int getActiveUsers() {
    return activeUsers.value();
  }
  
  public int getActiveApps() {
    return activeApplications.value();
  }
  
  public MetricsSystem getMetricsSystem() {
    return metricsSystem;
  }

  public long getAggregateNodeLocalContainersAllocated() {
    return aggregateNodeLocalContainersAllocated.value();
  }

  public long getAggregateRackLocalContainersAllocated() {
    return aggregateRackLocalContainersAllocated.value();
  }

  public long getAggregateOffSwitchContainersAllocated() {
    return aggregateOffSwitchContainersAllocated.value();
  }
}
