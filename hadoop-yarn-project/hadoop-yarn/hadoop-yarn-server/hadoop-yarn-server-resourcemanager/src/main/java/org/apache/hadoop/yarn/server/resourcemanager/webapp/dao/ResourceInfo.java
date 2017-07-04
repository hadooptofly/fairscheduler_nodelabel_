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

import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Map;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ResourceInfo {
  Map<String, Resource> resource;

  public ResourceInfo() {
  }

  public ResourceInfo(Map<String, Resource> res) {
    this.resource = res;
  }

  public int getMemory(String nodeLabel) {
    return resource.get(nodeLabel).getMemory();
  }

  public int getvCores(String nodeLabel) {
    return resource.get(nodeLabel).getVirtualCores();
  }

  public int getgCores(String nodeLabel) {
    return resource.get(nodeLabel).getGpuCores();
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String nodeLabel : resource.keySet()) {
      sb.append("<Label> " + nodeLabel);
      sb.append("<memory:" + resource.get(nodeLabel).getMemory()
          + ", vCores:" + resource.get(nodeLabel).getVirtualCores()
          + ", gCores:" + resource.get(nodeLabel).getGpuCores() + ">");
      sb.append("\n");
    }
    return sb.toString();
  }

  public void setMemory(int memory, String nodeLabel) {
    resource.get(nodeLabel).setMemory(memory);
  }

  public void setvCores(int vCores, String nodeLabel) {
    resource.get(nodeLabel).setVirtualCores(vCores);
  }

  public void setgCores(int gCores, String nodeLabel) {
    resource.get(nodeLabel).setGpuCores(gCores);
  }
}
