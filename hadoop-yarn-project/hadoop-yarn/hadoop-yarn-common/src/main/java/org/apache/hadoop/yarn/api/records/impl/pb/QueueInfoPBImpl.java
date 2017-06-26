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

package org.apache.hadoop.yarn.api.records.impl.pb;

import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.MockMap;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStateProto;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class QueueInfoPBImpl extends QueueInfo {

  QueueInfoProto proto = QueueInfoProto.getDefaultInstance();
  QueueInfoProto.Builder builder = null;
  boolean viaProto = false;

  List<ApplicationReport> applicationsList;
  List<QueueInfo> childQueuesList;
  Set<String> accessibleNodeLabels;
  MockMap capacity;
  MockMap currentCapacity;
  MockMap maximumCapacity;
  
  public QueueInfoPBImpl() {
    builder = QueueInfoProto.newBuilder();
  }
  
  public QueueInfoPBImpl(QueueInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public List<ApplicationReport> getApplications() {
    initLocalApplicationsList();
    return this.applicationsList;
  }

  @Override
  public MockMap getCapacity() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasCapacity()) {
      return null;
    }
    return new MockMapPBImpl(p.getCapacity());
  }

  @Override
  public List<QueueInfo> getChildQueues() {
    initLocalChildQueuesList();
    return this.childQueuesList;
  }

  @Override
  public MockMap getCurrentCapacity() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasCurrentCapacity()) {
      return null;
    }
    return new MockMapPBImpl(p.getCurrentCapacity());
  }

  @Override
  public MockMap getMaximumCapacity() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMaximumCapacity()) {
      return null;
    }
    return new MockMapPBImpl(p.getMaximumCapacity());
  }

  @Override
  public String getQueueName() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasQueueName()) ? p.getQueueName() : null;
  }

  @Override
  public QueueState getQueueState() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public void setApplications(List<ApplicationReport> applications) {
    if (applications == null) {
      builder.clearApplications();
    }
    this.applicationsList = applications;
  }

  @Override
  public void setCapacity(MockMap capacity) {
    maybeInitBuilder();
    if (capacity == null) {
      builder.clearCapacity();
      return;
    }
    this.capacity = capacity;
  }

  @Override
  public void setChildQueues(List<QueueInfo> childQueues) {
    if (childQueues == null) {
      builder.clearChildQueues();
    }
    this.childQueuesList = childQueues;
  }

  @Override
  public void setCurrentCapacity(MockMap currentCapacity) {
    maybeInitBuilder();
    if (currentCapacity == null) {
      builder.clearCurrentCapacity();
      return;
    }
    this.currentCapacity = currentCapacity;
  }

  @Override
  public void setMaximumCapacity(MockMap maximumCapacity) {
    maybeInitBuilder();
    if (maximumCapacity == null) {
      builder.clearMaximumCapacity();
      return;
    }
    this.maximumCapacity = maximumCapacity;
  }

  @Override
  public void setQueueName(String queueName) {
    maybeInitBuilder();
    if (queueName == null) {
      builder.clearQueueName();
      return;
    }
    builder.setQueueName(queueName);
  }

  @Override
  public void setQueueState(QueueState queueState) {
    maybeInitBuilder();
    if (queueState == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(queueState));
  }

  public QueueInfoProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void initLocalApplicationsList() {
    if (this.applicationsList != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationReportProto> list = p.getApplicationsList();
    applicationsList = new ArrayList<ApplicationReport>();

    for (ApplicationReportProto a : list) {
      applicationsList.add(convertFromProtoFormat(a));
    }
  }

  private void addApplicationsToProto() {
    maybeInitBuilder();
    builder.clearApplications();
    if (applicationsList == null)
      return;
    Iterable<ApplicationReportProto> iterable = new Iterable<ApplicationReportProto>() {
      @Override
      public Iterator<ApplicationReportProto> iterator() {
        return new Iterator<ApplicationReportProto>() {
  
          Iterator<ApplicationReport> iter = applicationsList.iterator();
  
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
  
          @Override
          public ApplicationReportProto next() {
            return convertToProtoFormat(iter.next());
          }
  
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
  
          }
        };
  
      }
    };
    builder.addAllApplications(iterable);
  }

  private void initLocalChildQueuesList() {
    if (this.childQueuesList != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    List<QueueInfoProto> list = p.getChildQueuesList();
    childQueuesList = new ArrayList<QueueInfo>();

    for (QueueInfoProto a : list) {
      childQueuesList.add(convertFromProtoFormat(a));
    }
  }

  private void addChildQueuesInfoToProto() {
    maybeInitBuilder();
    builder.clearChildQueues();
    if (childQueuesList == null)
      return;
    Iterable<QueueInfoProto> iterable = new Iterable<QueueInfoProto>() {
      @Override
      public Iterator<QueueInfoProto> iterator() {
        return new Iterator<QueueInfoProto>() {
  
          Iterator<QueueInfo> iter = childQueuesList.iterator();
  
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
  
          @Override
          public QueueInfoProto next() {
            return convertToProtoFormat(iter.next());
          }
  
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
  
          }
        };
  
      }
    };
    builder.addAllChildQueues(iterable);
  }

  private void mergeLocalToBuilder() {
    if (this.childQueuesList != null) {
      addChildQueuesInfoToProto();
    }
    if (this.applicationsList != null) {
      addApplicationsToProto();
    }
    if (this.accessibleNodeLabels != null) {
      builder.clearAccessibleNodeLabels();
      builder.addAllAccessibleNodeLabels(this.accessibleNodeLabels);
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = QueueInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }


  private ApplicationReportPBImpl convertFromProtoFormat(ApplicationReportProto a) {
    return new ApplicationReportPBImpl(a);
  }

  private ApplicationReportProto convertToProtoFormat(ApplicationReport t) {
    return ((ApplicationReportPBImpl)t).getProto();
  }

  private QueueInfoPBImpl convertFromProtoFormat(QueueInfoProto a) {
    return new QueueInfoPBImpl(a);
  }
  
  private QueueInfoProto convertToProtoFormat(QueueInfo q) {
    return ((QueueInfoPBImpl)q).getProto();
  }

  private QueueState convertFromProtoFormat(QueueStateProto q) {
    return ProtoUtils.convertFromProtoFormat(q);
  }
  
  private QueueStateProto convertToProtoFormat(QueueState queueState) {
    return ProtoUtils.convertToProtoFormat(queueState);
  }
  
  @Override
  public void setAccessibleNodeLabels(Set<String> nodeLabels) {
    maybeInitBuilder();
    builder.clearAccessibleNodeLabels();
    this.accessibleNodeLabels = nodeLabels;
  }
  
  private void initNodeLabels() {
    if (this.accessibleNodeLabels != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    this.accessibleNodeLabels = new HashSet<String>();
    this.accessibleNodeLabels.addAll(p.getAccessibleNodeLabelsList());
  }

  @Override
  public Set<String> getAccessibleNodeLabels() {
    initNodeLabels();
    return this.accessibleNodeLabels;
  }

  @Override
  public String getDefaultNodeLabelExpression() {
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasDefaultNodeLabelExpression()) ? p
        .getDefaultNodeLabelExpression().trim() : null;
  }

  @Override
  public void setDefaultNodeLabelExpression(String defaultNodeLabelExpression) {
    maybeInitBuilder();
    if (defaultNodeLabelExpression == null) {
      builder.clearDefaultNodeLabelExpression();
      return;
    }
    builder.setDefaultNodeLabelExpression(defaultNodeLabelExpression);
  }
}
