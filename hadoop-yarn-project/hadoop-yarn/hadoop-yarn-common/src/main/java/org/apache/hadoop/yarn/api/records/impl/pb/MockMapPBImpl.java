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
import org.apache.hadoop.yarn.api.protocolrecords.KV;
import org.apache.hadoop.yarn.api.protocolrecords.MockMap;
import org.apache.hadoop.yarn.proto.YarnProtos;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MockMapPBImpl extends MockMap{
  YarnProtos.MockMap proto = YarnProtos.MockMap.getDefaultInstance();
  YarnProtos.MockMap.Builder builder = null;
  boolean viaProto = false;

  private List<KV> entrys;

  public MockMapPBImpl() { this.builder = YarnProtos.MockMap.newBuilder(); }
  public MockMapPBImpl(YarnProtos.MockMap proto) {
    this.proto = proto;
    viaProto = true;
  }

  private void initEntrys() {
    if (entrys != null) {
      return;
    }
    YarnProtos.MockMapOrBuilder p = viaProto ? proto : builder;
    List<YarnProtos.KV> kvs = p.getEntryList();
    entrys = new ArrayList<KV>();
    for (YarnProtos.KV kv : kvs) {
      entrys.add(new KVPBImpl(kv));
    }
  }

  @Override
  public List<KV> getEntrys() {
    initEntrys();
    return this.entrys;
  }

  @Override
  public void setEntrys(List<KV> entrys) {
    if (entrys == null) {
      builder.clearEntry();
    }
    this.entrys = entrys;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnProtos.MockMap.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addEntrysToProto() {
    maybeInitBuilder();
    builder.clearEntry();
    if (entrys == null) {
      return;
    }

    Iterable<YarnProtos.KV> iterable = new Iterable<YarnProtos.KV>() {
      @Override
      public Iterator<YarnProtos.KV> iterator()
      {
        return new Iterator<YarnProtos.KV>() {
          Iterator<KV> iter = entrys.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public YarnProtos.KV next() {
            return ((KVPBImpl) iter.next()).getProto();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };

    builder.addAllEntry(iterable);
  }

  private void mergeLocalToBuilder() {
    if (this.entrys != null) {
      addEntrysToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  public YarnProtos.MockMap getProto() {
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

}
