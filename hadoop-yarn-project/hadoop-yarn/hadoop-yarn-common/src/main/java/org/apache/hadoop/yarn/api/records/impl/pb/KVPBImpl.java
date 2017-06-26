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

import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.protocolrecords.KV;
import org.apache.hadoop.yarn.proto.YarnProtos;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class KVPBImpl extends KV {
  YarnProtos.KV kvProto = YarnProtos.KV.getDefaultInstance();
  YarnProtos.KV.Builder builder = null;
  boolean viaProto = false;

  private String key = "NULL";
  private float value = -1.0f;

  public KVPBImpl() {this.builder = YarnProtos.KV.newBuilder();}
  public KVPBImpl(YarnProtos.KV kvProto) {
    this.kvProto = kvProto;
    viaProto = true;
  }

  @Override
  public String getKey() {
    if (!this.key.equals("NULL")) {
      return this.key;
    }

    YarnProtos.KVOrBuilder p = viaProto ? kvProto : builder;
    if (!p.hasK()) {
      return null;
    }

    this.key = p.getK();
    return this.key;
  }

  @Override
  public float getValue() {
    if (this.value != -1.0f) {
      return this.value;
    }

    YarnProtos.KVOrBuilder p = viaProto ? kvProto : builder;
    if (!p.hasV()) {
      return -1.0f;
    }

    this.value = p.getV();
    return this.value;
  }

  @Override
  public void setKey(String key) {
    maybeInitBuilder();
    if (this.key.equals("NULL")) {
      builder.clearK();
    }

    this.key = key;
  }

  @Override
  public void setValue(float value) {
    maybeInitBuilder();
    if (this.value == -1.0f) {
      builder.clearV();
    }

    this.value = value;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnProtos.KV.newBuilder(kvProto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.key != null) {
      builder.setK(key);
    }

    if (this.value != -0.1f) {
      builder.setV(value);
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    kvProto = builder.build();
    viaProto = true;
  }

  public YarnProtos.KV getProto() {
    mergeLocalToProto();
    kvProto = viaProto ? kvProto : builder.build();
    viaProto = true;
    return kvProto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
