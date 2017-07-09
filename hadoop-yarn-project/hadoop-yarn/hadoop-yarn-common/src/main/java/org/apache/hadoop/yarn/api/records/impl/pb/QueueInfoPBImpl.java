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
  Map<String, Float> capacity;
  Map<String, Float> currentCapacity;
  Map<String, Float> maximumCapacity;
  
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
  public Map<String, Float> getCapacity() {
    initCapacity();
    return capacity;
  }

  @Override
  public List<QueueInfo> getChildQueues() {
    initLocalChildQueuesList();
    return this.childQueuesList;
  }

  @Override
  public Map<String, Float> getCurrentCapacity() {
    initCurrentCapacity();
    return currentCapacity;
  }

  @Override
  public Map<String, Float> getMaximumCapacity() {
    initMaxCapacity();
    return maximumCapacity;
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
  public void setCapacity(Map<String, Float> capacity) {
    if (capacity == null)
      return;
    initCapacity();
    this.capacity.clear();
    this.capacity.putAll(capacity);
  }

  @Override
  public void setChildQueues(List<QueueInfo> childQueues) {
    if (childQueues == null) {
      builder.clearChildQueues();
    }
    this.childQueuesList = childQueues;
  }

  @Override
  public void setCurrentCapacity(Map<String, Float> currentCapacity) {
    if (currentCapacity == null)
      return;
    initCurrentCapacity();
    currentCapacity.clear();
    this.currentCapacity.putAll(currentCapacity);
  }

  @Override
  public void setMaximumCapacity(Map<String, Float> maximumCapacity) {
    if (maximumCapacity == null)
      return;
    initMaxCapacity();
    this.maximumCapacity.clear();
    this.maximumCapacity.putAll(maximumCapacity);
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


  private void initCapacity() {
    if (capacity != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    capacity = new HashMap<String, Float>();
    List<YarnProtos.StringFloatMapProto> protos = p.getCapacityList();
    for (YarnProtos.StringFloatMapProto sfp : protos) {
      capacity.put(sfp.getK(), sfp.getV());
    }
  }

  private void addCapacityToProto() {
    maybeInitBuilder();
    builder.clearCapacity();
    if (capacity == null)
      return;
    Iterable<YarnProtos.StringFloatMapProto> iterable =
        new Iterable<YarnProtos.StringFloatMapProto>() {
          @Override
          public Iterator<YarnProtos.StringFloatMapProto> iterator() {
            return new Iterator<YarnProtos.StringFloatMapProto>() {

              Iterator<String> iterator = capacity.keySet().iterator();

              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public YarnProtos.StringFloatMapProto next() {
                String key = iterator.next();
                return YarnProtos.StringFloatMapProto.newBuilder().setK(key)
                    .setV(capacity.get(key)).build();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
    builder.addAllCapacity(iterable);
  }


  private void initCurrentCapacity() {
    if (capacity != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    currentCapacity = new HashMap<String, Float>();
    List<YarnProtos.StringFloatMapProto> protos = p.getCurrentCapacityList();
    for (YarnProtos.StringFloatMapProto sfp : protos) {
      currentCapacity.put(sfp.getK(), sfp.getV());
    }
  }

  private void addCurrentCapacityToProto() {
    maybeInitBuilder();
    builder.clearCapacity();
    if (currentCapacity == null)
      return;
    Iterable<YarnProtos.StringFloatMapProto> iterable =
        new Iterable<YarnProtos.StringFloatMapProto>() {
          @Override
          public Iterator<YarnProtos.StringFloatMapProto> iterator() {
            return new Iterator<YarnProtos.StringFloatMapProto>() {

              Iterator<String> iterator = currentCapacity.keySet().iterator();

              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public YarnProtos.StringFloatMapProto next() {
                String key = iterator.next();
                return YarnProtos.StringFloatMapProto.newBuilder().setK(key)
                    .setV(currentCapacity.get(key)).build();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
    builder.addAllCurrentCapacity(iterable);
  }

  private void initMaxCapacity() {
    if (maximumCapacity != null) {
      return;
    }
    QueueInfoProtoOrBuilder p = viaProto ? proto : builder;
    maximumCapacity = new HashMap<String, Float>();
    List<YarnProtos.StringFloatMapProto> protos = p.getMaximumCapacityList();
    for (YarnProtos.StringFloatMapProto sfp : protos) {
      maximumCapacity.put(sfp.getK(), sfp.getV());
    }
  }

  private void addMaxCapacityToProto() {
    maybeInitBuilder();
    builder.clearCapacity();
    if (maximumCapacity == null)
      return;
    Iterable<YarnProtos.StringFloatMapProto> iterable =
        new Iterable<YarnProtos.StringFloatMapProto>() {
          @Override
          public Iterator<YarnProtos.StringFloatMapProto> iterator() {
            return new Iterator<YarnProtos.StringFloatMapProto>() {

              Iterator<String> iterator = maximumCapacity.keySet().iterator();

              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public YarnProtos.StringFloatMapProto next() {
                String key = iterator.next();
                return YarnProtos.StringFloatMapProto.newBuilder().setK(key)
                    .setV(maximumCapacity.get(key)).build();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
    builder.addAllMaximumCapacity(iterable);
  }

  private void mergeLocalToBuilder() {
    if (capacity != null) {
      addCapacityToProto();
    }
    if (currentCapacity != null) {
      addCurrentCapacityToProto();
    }
    if (maximumCapacity != null) {
      addMaxCapacityToProto();
    }
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

  private QueueInfoPBImpl convertFromProtoFormat(QueueInfoProto q) {
    return new QueueInfoPBImpl(q);
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
