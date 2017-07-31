package org.apache.hadoop.tools.metrics;

import com.sun.jdi.InvalidTypeException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.lib.*;

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

    public V get(Object k, MetricsInfo info) throws InvalidTypeException{
        V v = super.get(k);
        if (null == v) {
            switch (v.getClass().getName())
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
                            info, new NoNullHashMap<String, MutableCounterInt>());
                    put((K) k, (V) mutableMapCounterInt);
                    return (V) mutableMapCounterInt;
                case "org.apache.hadoop.metrics2.lib.MutableMapCounterLong" :
                    MutableMapCounterLong mutableMapCounterLong = new MutableMapCounterLong(
                            info, new NoNullHashMap<String, MutableCounterLong>());
                    put((K) k, (V) mutableMapCounterLong);
                    return (V) mutableMapCounterLong;
                case "org.apache.hadoop.metrics2.lib.MutableMapGaugeInt" :
                    MutableMapGaugeInt mutableMapGaugeInt = new MutableMapGaugeInt(
                            info, new NoNullHashMap<String, MutableGaugeInt>());
                    put((K) k, (V) mutableMapGaugeInt);
                    return (V) mutableMapGaugeInt;
                case "org.apache.hadoop.metrics2.lib.MutableMapGaugeLong" :
                    MutableMapGaugeLong mutableMapGaugeLong = new MutableMapGaugeLong(
                            info, new NoNullHashMap<String, MutableGaugeLong>());
                    put((K) k, (V) mutableMapGaugeLong);
                    return (V) mutableMapGaugeLong;
                default:
                    throw new InvalidTypeException("Haven`t support this type"
                            + v.getClass().getName()
                            + " to use as value.");
            }
        }
        return v;
    }
}
