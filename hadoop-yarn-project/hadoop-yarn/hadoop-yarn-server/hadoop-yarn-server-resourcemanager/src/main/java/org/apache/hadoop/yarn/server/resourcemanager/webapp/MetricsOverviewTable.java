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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.UserMetricsInfo;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * Provides an table with an overview of many cluster wide metrics and if
 * per user metrics are enabled it will show an overview of what the
 * current user is using on the cluster.
 */
public class MetricsOverviewTable extends HtmlBlock {
  private static final long BYTES_IN_MB = 1024 * 1024;

  private final ResourceManager rm;

  @Inject
  MetricsOverviewTable(ResourceManager rm, ViewContext ctx) {
    super(ctx);
    this.rm = rm;
  }


  @Override
  protected void render(Block html) {
    //Yes this is a hack, but there is no other way to insert
    //CSS in the correct spot
    html.style(".metrics {margin-bottom:5px}"); 
    
    ClusterMetricsInfo clusterMetrics = new ClusterMetricsInfo(this.rm);
    
    DIV<Hamlet> div = html.div().$class("metrics");

    StringBuilder allocateContainer = new StringBuilder();
    StringBuilder reserveMB = new StringBuilder();
    StringBuilder allocateMB = new StringBuilder();
    StringBuilder totalMB = new StringBuilder();
    StringBuilder reserveVcore = new StringBuilder();
    StringBuilder allocateVcore = new StringBuilder();
    StringBuilder totalVcore = new StringBuilder();
    StringBuilder reserveGcore = new StringBuilder();
    StringBuilder allocateGcore = new StringBuilder();
    StringBuilder totalGcore = new StringBuilder();

    for (String nodeLabel : rm.getRMContext().getNodeLabelManager().getLabelSet()) {
      MutableGaugeInt allocContainer = clusterMetrics.getContainersAllocated().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
      MutableGaugeInt allocMB = clusterMetrics.getAllocatedMB().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
      MutableGaugeInt resMB = clusterMetrics.getReservedMB().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
      MutableGaugeInt allocVcore = clusterMetrics.getAllocatedVirtualCores().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
      MutableGaugeInt resVcore = clusterMetrics.getReservedVirtualCores().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
      MutableGaugeInt allocGcore = clusterMetrics.getAllocatedGpuCores().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
      MutableGaugeInt resGcore = clusterMetrics.getReservedGpuCores().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);

      allocateContainer.append(nodeLabel).append(": ").append(String
              .valueOf(allocContainer.value()))
              .append("\n");
      allocateMB.append(nodeLabel).append(": ").append(StringUtils
              .byteDesc(((long)allocMB.value()) * BYTES_IN_MB))
              .append("\n");
      reserveMB.append(nodeLabel).append(": ").append(StringUtils
              .byteDesc(((long )resMB.value()) * BYTES_IN_MB))
              .append("\n");
      totalMB.append(nodeLabel).append(": ").append(StringUtils
              .byteDesc( clusterMetrics.getTotalMB().get(nodeLabel).longValue() * BYTES_IN_MB))
              .append("\n");
      allocateVcore.append(nodeLabel).append(": ").append(String.valueOf(
              allocVcore.value()))
              .append("\n");
      reserveVcore.append(nodeLabel).append(": ").append(String.valueOf(
              resVcore.value()))
              .append("\n");
      totalVcore.append(nodeLabel).append(": ").append(String.valueOf(
              clusterMetrics.getTotalVirtualCores().get(nodeLabel)))
              .append("\n");
      allocateGcore.append(nodeLabel).append(": ").append(String.valueOf(
              allocGcore.value()))
              .append("\n");
      reserveGcore.append(nodeLabel).append(": ").append(String.valueOf(
              resGcore.value()))
              .append("\n");
      totalGcore.append(nodeLabel).append(": ").append(String.valueOf(
              clusterMetrics.getTotalGpuCores().get(nodeLabel)))
              .append("\n");
    }
    
    div.h3("Cluster Metrics").
    table("#metricsoverview").
    thead().$class("ui-widget-header").
      tr().
        th().$class("ui-state-default")._("Apps Submitted")._().
        th().$class("ui-state-default")._("Apps Pending")._().
        th().$class("ui-state-default")._("Apps Running")._().
        th().$class("ui-state-default")._("Apps Completed")._().
        th().$class("ui-state-default")._("Containers Running")._().
        th().$class("ui-state-default")._("Memory Used")._().
        th().$class("ui-state-default")._("Memory Total")._().
        th().$class("ui-state-default")._("Memory Reserved")._().
        th().$class("ui-state-default")._("VCores Used")._().
        th().$class("ui-state-default")._("VCores Total")._().
        th().$class("ui-state-default")._("VCores Reserved")._().
        th().$class("ui-state-default")._("GCores Used")._().
        th().$class("ui-state-default")._("GCores Total")._().
        th().$class("ui-state-default")._("GCores Reserved")._().
        th().$class("ui-state-default")._("Active Nodes")._().
        th().$class("ui-state-default")._("Decommissioned Nodes")._().
        th().$class("ui-state-default")._("Lost Nodes")._().
        th().$class("ui-state-default")._("Unhealthy Nodes")._().
        th().$class("ui-state-default")._("Rebooted Nodes")._().
      _().
    _().
    tbody().$class("ui-widget-content").
      tr().
        td(String.valueOf(clusterMetrics.getAppsSubmitted())).
        td(String.valueOf(clusterMetrics.getAppsPending())).
        td(String.valueOf(clusterMetrics.getAppsRunning())).
        td(
            String.valueOf(
                clusterMetrics.getAppsCompleted() + 
                clusterMetrics.getAppsFailed() + clusterMetrics.getAppsKilled()
                )
          ).
        td(allocateContainer.toString()).
        td(allocateMB.toString()).
        td(totalMB.toString()).
        td(reserveMB.toString()).
        td(allocateVcore.toString()).
        td(totalVcore.toString()).
        td(reserveVcore.toString()).
        td(allocateGcore.toString()).
        td(totalGcore.toString()).
        td(reserveGcore.toString()).
        td().a(url("nodes"),String.valueOf(clusterMetrics.getActiveNodes()))._().
        td().a(url("nodes/decommissioned"),String.valueOf(clusterMetrics.getDecommissionedNodes()))._().
        td().a(url("nodes/lost"),String.valueOf(clusterMetrics.getLostNodes()))._().
        td().a(url("nodes/unhealthy"),String.valueOf(clusterMetrics.getUnhealthyNodes()))._().
        td().a(url("nodes/rebooted"),String.valueOf(clusterMetrics.getRebootedNodes()))._().
      _().
    _()._();
    
    String user = request().getRemoteUser();
    if (user != null) {
      UserMetricsInfo userMetrics = new UserMetricsInfo(this.rm, user);
      if (userMetrics.metricsAvailable()) {
        StringBuilder userAllocateContainer = new StringBuilder();
        StringBuilder userPendingContainer = new StringBuilder();
        StringBuilder userReserveContainer = new StringBuilder();
        StringBuilder userAllocateMB = new StringBuilder();
        StringBuilder userPendingMB = new StringBuilder();
        StringBuilder userReserveMB = new StringBuilder();
        StringBuilder userAllocateVcore = new StringBuilder();
        StringBuilder userPendingVcore = new StringBuilder();
        StringBuilder userReserveVcore = new StringBuilder();
        StringBuilder userAllocateGcore = new StringBuilder();
        StringBuilder userPendingGcore = new StringBuilder();
        StringBuilder userReserveGcore = new StringBuilder();

        for (String nodeLabel : rm.getRMContext().getNodeLabelManager().getLabelSet()) {
          MutableGaugeInt allocContainer = userMetrics.getRunningContainers().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
          MutableGaugeInt pendContainer = userMetrics.getPendingContainers().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
          MutableGaugeInt reserveContainer = userMetrics.getReservedContainers().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
          MutableGaugeInt allocMB = userMetrics.getAllocatedMB().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
          MutableGaugeInt pendMB = userMetrics.getPendingMB().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
          MutableGaugeInt resMB = userMetrics.getReservedMB().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
          MutableGaugeInt allocVcore = userMetrics.getAllocatedVirtualCores().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
          MutableGaugeInt pendVcore = userMetrics.getPendingVirtualCores().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
          MutableGaugeInt resVcore = userMetrics.getReservedVirtualCores().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
          MutableGaugeInt allocGcore = userMetrics.getAllocatedGpuCores().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
          MutableGaugeInt pendGcore = userMetrics.getPendingGpuCores().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);
          MutableGaugeInt resGcore = userMetrics.getReservedGpuCores().getValue().get(nodeLabel, QueueMetrics.QUEUE_INFO);


          userAllocateContainer.append(nodeLabel).append(": ").append(String.valueOf(
                  allocContainer != null ? allocContainer.value() : 0))
                  .append("\n");
          userPendingContainer.append(nodeLabel).append(": ").append(String.valueOf(
                  pendContainer != null ? pendContainer.value() : 0))
                  .append("\n");
          userReserveContainer.append(nodeLabel).append(": ").append(String.valueOf(
                  reserveContainer != null ? reserveContainer.value() : 0))
                  .append("\n");
          userAllocateMB.append(nodeLabel).append(": ").append(StringUtils.byteDesc(
                  allocMB != null ? allocMB.value() * BYTES_IN_MB : 0))
                  .append("\n");
          userPendingMB.append(nodeLabel).append(": ").append(StringUtils.byteDesc(
                  pendMB != null ? pendMB.value() * BYTES_IN_MB : 0))
                  .append("\n");
          userReserveMB.append(nodeLabel).append(": ").append(StringUtils.byteDesc(
                  resMB != null ? resMB.value() * BYTES_IN_MB : 0))
                  .append("\n");
          userAllocateVcore.append(nodeLabel).append(": ").append(String.valueOf(
                  allocVcore != null ? allocVcore.value() : 0))
                  .append("\n");
          userPendingVcore.append(nodeLabel).append(": ").append(String.valueOf(
                  pendVcore != null ? pendVcore.value() : 0))
                  .append("\n");
          userReserveVcore.append(nodeLabel).append(": ").append(String.valueOf(
                  resVcore != null ? resVcore.value() : 0))
                  .append("\n");
          userAllocateGcore.append(nodeLabel).append(": ").append(String.valueOf(
                  allocGcore != null ? allocGcore.value() : 0))
                  .append("\n");
          userPendingGcore.append(nodeLabel).append(": ").append(String.valueOf(
                  pendGcore != null ? pendGcore.value() : 0))
                  .append("\n");
          userReserveGcore.append(nodeLabel).append(": ").append(String.valueOf(
                  resGcore != null ? resGcore.value() : 0))
                  .append("\n");
        }

        div.h3("User Metrics for " + user).
        table("#usermetricsoverview").
        thead().$class("ui-widget-header").
          tr().
            th().$class("ui-state-default")._("Apps Submitted")._().
            th().$class("ui-state-default")._("Apps Pending")._().
            th().$class("ui-state-default")._("Apps Running")._().
            th().$class("ui-state-default")._("Apps Completed")._().
            th().$class("ui-state-default")._("Containers Running")._().
            th().$class("ui-state-default")._("Containers Pending")._().
            th().$class("ui-state-default")._("Containers Reserved")._().
            th().$class("ui-state-default")._("Memory Used")._().
            th().$class("ui-state-default")._("Memory Pending")._().
            th().$class("ui-state-default")._("Memory Reserved")._().
            th().$class("ui-state-default")._("VCores Used")._().
            th().$class("ui-state-default")._("VCores Pending")._().
            th().$class("ui-state-default")._("VCores Reserved")._().
            th().$class("ui-state-default")._("GCores Used")._().
            th().$class("ui-state-default")._("GCores Pending")._().
            th().$class("ui-state-default")._("GCores Reserved")._().
          _().
        _().
        tbody().$class("ui-widget-content").
          tr().
            td(String.valueOf(userMetrics.getAppsSubmitted())).
            td(String.valueOf(userMetrics.getAppsPending())).
            td(String.valueOf(userMetrics.getAppsRunning())).
            td(
                String.valueOf(
                    (userMetrics.getAppsCompleted() + 
                     userMetrics.getAppsFailed() + userMetrics.getAppsKilled())
                    )
              ).
            td(userAllocateContainer.toString()).
            td(userPendingContainer.toString()).
            td(userReserveContainer.toString()).
            td(userAllocateMB.toString()).
            td(userPendingMB.toString()).
            td(userReserveMB.toString()).
            td(userAllocateVcore.toString()).
            td(userPendingVcore.toString()).
            td(userReserveVcore.toString()).
            td(userAllocateGcore.toString()).
            td(userPendingGcore.toString()).
            td(userReserveGcore.toString()).
          _().
        _()._();
        
      }
    }
    
    SchedulerInfo schedulerInfo=new SchedulerInfo(this.rm);
    
    div.h3("Scheduler Metrics").
    table("#schedulermetricsoverview").
    thead().$class("ui-widget-header").
      tr().
        th().$class("ui-state-default")._("Scheduler Type")._().
        th().$class("ui-state-default")._("Scheduling Resource Type")._().
        th().$class("ui-state-default")._("Minimum Allocation")._().
        th().$class("ui-state-default")._("Maximum Allocation")._().
      _().
    _().
    tbody().$class("ui-widget-content").
      tr().
        td(String.valueOf(schedulerInfo.getSchedulerType())).
        td(String.valueOf(schedulerInfo.getSchedulerResourceTypes())).
        td(schedulerInfo.getMinAllocation().toString()).
        td(schedulerInfo.getMaxAllocation().toString()).
      _().
    _()._();

    div._();
  }
}
