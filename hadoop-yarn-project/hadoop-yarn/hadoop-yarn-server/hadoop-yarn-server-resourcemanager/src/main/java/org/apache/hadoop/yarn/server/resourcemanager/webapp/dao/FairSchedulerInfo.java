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
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.NodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@XmlRootElement(name = "fairScheduler")
@XmlType(name = "fairScheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class FairSchedulerInfo extends SchedulerInfo {
  public static final int INVALID_FAIR_SHARE = -1;
  private FairSchedulerQueueInfo rootQueue;
  
  @XmlTransient
  private FairScheduler scheduler;

  private FairSchedulerQueueInfoList queues;
  public FairSchedulerInfo() {
  } // JAXB needs this
  
  public FairSchedulerInfo(FairScheduler fs) {
    scheduler = fs;
    rootQueue = new FairSchedulerQueueInfo(scheduler.getQueueManager().
            getRootQueue(), scheduler, RMNodeLabelsManager.NO_LABEL);
  }

  public FairSchedulerInfo(FairScheduler fs,FSQueue parent, NodeLabel nodeLabel) {
    scheduler = fs;
    rootQueue = new FairSchedulerQueueInfo(scheduler.getQueueManager().
            getRootQueue(), scheduler, nodeLabel.getLabelName());

    this.queues = getQueues(parent, nodeLabel);
  }

  /**
   * Get the fair share assigned to the appAttemptId.
   * @param appAttemptId
   * @return The fair share assigned to the appAttemptId,
   * <code>FairSchedulerInfo#INVALID_FAIR_SHARE</code> if the scheduler does
   * not know about this application attempt.
   */
  public Map<String, Resource> getAppFairShare(ApplicationAttemptId appAttemptId) {
    FSAppAttempt fsAppAttempt = scheduler.getSchedulerApp(appAttemptId);
    return fsAppAttempt == null ?
        null :  fsAppAttempt.getFairShare();
  }

  public FairSchedulerQueueInfoList getQueues(FSQueue parent, NodeLabel nodeLabel) {
    FairSchedulerQueueInfoList queuesInfo =
            new FairSchedulerQueueInfoList();

    // JAXB marashalling leads to situation where the "type" field injected
    // for JSON changes from string to array depending on order of printing
    // Issue gets fixed if all the leaf queues are marshalled before the
    // non-leaf queues. See YARN-4785 for more details.
    List<FSQueue> childQueues = new ArrayList<>();
    List<FSQueue> childLeafQueues = new ArrayList<>();
    List<FSQueue> childNonLeafQueues = new ArrayList<>();
    for (FSQueue queue : parent.getChildQueues()) {
      if (!((FSQueue) queue).accessibleToPartition(nodeLabel
              .getLabelName())) {
        // Skip displaying the hierarchy for the queues for which the
        // labels are not accessible
        continue;
      }
      if (queue instanceof FSLeafQueue) {
        childLeafQueues.add(queue);
      } else {
        childNonLeafQueues.add(queue);
      }
    }
    childQueues.addAll(childLeafQueues);
    childQueues.addAll(childNonLeafQueues);

    for (FSQueue queue : childQueues) {
      FairSchedulerQueueInfo info;
      if (queue instanceof FSLeafQueue) {
        info =
                new FairSchedulerLeafQueueInfo((FSLeafQueue) queue, scheduler,
                        nodeLabel.getLabelName());
      } else {
        info = new FairSchedulerQueueInfo(queue, scheduler, nodeLabel.getLabelName());
        info.queues = getQueues(queue, nodeLabel);
      }
      queuesInfo.addToQueueList(info);
    }
    return queuesInfo;
  }

  public FairSchedulerQueueInfoList getQueues() { return this.queues; }

  public FairSchedulerQueueInfo getRootQueueInfo() {
    return rootQueue;
  }
}
