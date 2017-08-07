package org.apache.hadoop.tools.metrics;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.impl.MetricCounterInt;
import org.apache.hadoop.metrics2.impl.MetricCounterLong;
import org.apache.hadoop.metrics2.impl.MetricGaugeDouble;
import org.apache.hadoop.metrics2.impl.MetricGaugeFloat;
import org.apache.hadoop.metrics2.impl.MetricGaugeInt;
import org.apache.hadoop.metrics2.impl.MetricGaugeLong;
import org.apache.hadoop.metrics2.impl.MetricMapCounterInt;
import org.apache.hadoop.metrics2.impl.MetricMapCounterLong;
import org.apache.hadoop.metrics2.impl.MetricMapGaugeDouble;
import org.apache.hadoop.metrics2.impl.MetricMapGaugeFloat;
import org.apache.hadoop.metrics2.impl.MetricMapGaugeInt;
import org.apache.hadoop.metrics2.impl.MetricMapGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableMapCounterInt;
import org.apache.hadoop.metrics2.lib.MutableMapCounterLong;
import org.apache.hadoop.metrics2.lib.MutableMapGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableMapGaugeLong;

import java.lang.reflect.ParameterizedType;
import java.security.InvalidParameterException;
import java.util.HashMap;

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

public class NoNullHashMap<K, V> extends HashMap<K, V>{

    private final String kClass, vClass;

    {
        kClass = ((Class)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[0]).getName();
        vClass = ((Class)((ParameterizedType)this.getClass().getGenericSuperclass()).getActualTypeArguments()[1]).getName();
    }

    public V get(Object k, MetricsInfo info) throws InvalidParameterException{
        V v = super.get(k);
        if (null == v) {
            switch (vClass)
            {
                case "org.apache.hadoop.metrics2.lib.MutableCounterInt" :
                    MutableCounterInt mutableCounterInt = new MutableCounterInt(info, 0);
                    put((K) k, (V) mutableCounterInt);
                    return (V) mutableCounterInt;
                case "org.apache.hadoop.metrics2.lib.MutableCounterLong" :
                    MutableCounterLong mutableCounterLong = new MutableCounterLong(info, 0L);
                    put((K) k, (V) mutableCounterLong);
                    return (V) mutableCounterLong;
                case "org.apache.hadoop.metrics2.lib.MutableGaugeInt" :
                    MutableGaugeInt mutableGaugeInt = new MutableGaugeInt(info, 0);
                    put((K) k, (V) v);
                    return (V) mutableGaugeInt;
                case "org.apache.hadoop.metrics2.lib.MutableGaugeLong" :
                    MutableGaugeLong mutableGaugeLong = new MutableGaugeLong(info, 0L);
                    put((K) k, (V) v);
                    return (V) mutableGaugeLong;
                case "org.apache.hadoop.metrics2.lib.MutableMapCounterInt" :
                    MutableMapCounterInt mutableMapCounterInt = new MutableMapCounterInt(
                            info, new NoNullHashMap<String, MutableCounterInt>(){});
                    put((K) k, (V) mutableMapCounterInt);
                    return (V) mutableMapCounterInt;
                case "org.apache.hadoop.metrics2.lib.MutableMapCounterLong" :
                    MutableMapCounterLong mutableMapCounterLong = new MutableMapCounterLong(
                            info, new NoNullHashMap<String, MutableCounterLong>(){});
                    put((K) k, (V) mutableMapCounterLong);
                    return (V) mutableMapCounterLong;
                case "org.apache.hadoop.metrics2.lib.MutableMapGaugeInt" :
                    MutableMapGaugeInt mutableMapGaugeInt = new MutableMapGaugeInt(
                            info, new NoNullHashMap<String, MutableGaugeInt>(){});
                    put((K) k, (V) mutableMapGaugeInt);
                    return (V) mutableMapGaugeInt;
                case "org.apache.hadoop.metrics2.lib.MutableMapGaugeLong" :
                    MutableMapGaugeLong mutableMapGaugeLong = new MutableMapGaugeLong(
                            info, new NoNullHashMap<String, MutableGaugeLong>(){});
                    put((K) k, (V) mutableMapGaugeLong);
                    return (V) mutableMapGaugeLong;
                case "org.apache.hadoop.metrics2.impl.MetricCounterInt" :
                    MetricCounterInt metricCounterInt = new MetricCounterInt(info, 0);
                    put((K) k,  (V) metricCounterInt);
                    return (V) metricCounterInt;
                case "org.apache.hadoop.metrics2.impl.MetricCounterLong" :
                    MetricCounterLong metricCounterLong = new MetricCounterLong(info, 0L);
                    put((K) k, (V) metricCounterLong);
                    return (V) metricCounterLong;
                case "org.apache.hadoop.metrics2.impl.MetricGaugeInt" :
                    MetricGaugeInt metricGaugeInt = new MetricGaugeInt(info, 0);
                    put((K) k, (V) metricGaugeInt);
                    return (V) metricGaugeInt;
                case "org.apache.hadoop.metrics2.impl.MetricGaugeLong" :
                    MetricGaugeLong metricGaugeLong = new MetricGaugeLong(info, 0L);
                    put((K) k, (V) metricGaugeLong);
                    return (V) metricGaugeLong;
                case "org.apache.hadoop.metrics2.impl.MetricGaugeFloat" :
                    MetricGaugeFloat metricGaugeFloat = new MetricGaugeFloat(info, 0.0f);
                    put((K) k, (V) metricGaugeFloat);
                    return (V) metricGaugeFloat;
                case "org.apache.hadoop.metrics2.impl.MetricGaugeDouble" :
                    MetricGaugeDouble metricGaugeDouble = new MetricGaugeDouble(info, 0.0);
                    put((K) k, (V) metricGaugeDouble);
                    return (V) metricGaugeDouble;
                case "org.apache.hadoop.metrics2.impl.MetricMapCounterInt" :
                    MetricMapCounterInt metricMapCounterInt = new MetricMapCounterInt(
                            info, new NoNullHashMap<String, MetricCounterInt>(){});
                    put((K) k,  (V) metricMapCounterInt);
                    return (V) metricMapCounterInt;
                case "org.apache.hadoop.metrics2.impl.MetricMapCounterLong" :
                    MetricMapCounterLong metricMapCounterLong = new MetricMapCounterLong(
                            info, new NoNullHashMap<String, MetricCounterLong>(){});
                    put((K) k, (V) metricMapCounterLong);
                    return (V) metricMapCounterLong;
                case "org.apache.hadoop.metrics2.impl.MetricMapGaugeInt" :
                    MetricMapGaugeInt metricMapGaugeInt = new MetricMapGaugeInt(
                            info, new NoNullHashMap<String, MetricGaugeInt>(){});
                    put((K) k, (V) metricMapGaugeInt);
                    return (V) metricMapGaugeInt;
                case "org.apache.hadoop.metrics2.impl.MetricMapGaugeLong" :
                    MetricMapGaugeLong metricMapGaugeLong = new MetricMapGaugeLong(
                            info, new NoNullHashMap<String, MetricGaugeLong>(){});
                    put((K) k, (V) metricMapGaugeLong);
                    return (V) metricMapGaugeLong;
                case "org.apache.hadoop.metrics2.impl.MetricMapGaugeFloat" :
                    MetricMapGaugeFloat metricMapGaugeFloat = new MetricMapGaugeFloat(
                            info, new NoNullHashMap<String, MetricGaugeFloat>(){});
                    put((K) k, (V) metricMapGaugeFloat);
                    return (V) metricMapGaugeFloat;
                case "org.apache.hadoop.metrics2.impl.MetricMapGaugeDouble" :
                    MetricMapGaugeDouble metricMapGaugeDouble = new MetricMapGaugeDouble(
                            info, new NoNullHashMap<String, MetricGaugeDouble>(){});
                    put((K) k, (V) metricMapGaugeDouble);
                    return (V) metricMapGaugeDouble;
                default:
                    throw new InvalidParameterException("Haven`t support this type"
                            + v.getClass().getName()
                            + " to use as value.");
            }
        }
        return v;
    }
}
