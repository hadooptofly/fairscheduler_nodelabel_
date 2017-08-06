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

package org.apache.hadoop.yarn.util;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceWeights;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;

public class NoNullHashMap<K, V> extends HashMap<K, V> {

    private final String kClass, vClass;

    {
        kClass = ((Class)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[0]).getName();
        vClass = ((Class)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[1]).getName();
    }

    @Override
    public V get(Object k) {
        V v = super.get(k);
        if (null == v) {
            switch (vClass) {
                case "org.apache.hadoop.yarn.api.records.Resource":
                    Resource resource = Resources.createResource(0, 0, 0);
                    put((K) k, (V) resource);
                    return (V) resource;
                case "org.apache.hadoop.yarn.util.resource.ResourceWeights":
                    ResourceWeights resourceWeights = new ResourceWeights(1, 1 , 1);
                    put((K) k, (V) resourceWeights);
                    return (V) resourceWeights;
                case "java.lang.Long":
                    Long l = 0L;
                    put((K) k, (V) l);
                    return (V) l;
                case "java.lang.Float":
                    Float f = 0f;
                    put((K) k, (V) f);
                    return (V) f;
                case "java.lang.Boolean":
                    Boolean b = false;
                    put((K) k, (V) b);
                    return (V) b;
            }
        }
        return v;
    }
}
