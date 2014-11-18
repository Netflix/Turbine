/**
 * Copyright 2014 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.turbine.aggregator;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.netflix.turbine.internal.AggregateStringSerializer;
import com.netflix.turbine.internal.JsonUtility;

/**
 * Represents the different sets of string values that have been received for a given key
 * and the respective value counts.
 * 
 * For example, if 2 "false" and 1 "true" were set on this the output would be: {"false":2,"true":1}
 * 
 * This class is optimized for the fact that almost all the time the value is expected to be the same and only rarely have more than 1 value.
 */
@JsonSerialize(using = AggregateStringSerializer.class)
public class AggregateString {

    private final Map<String, Integer> values;
    private final Set<InstanceKey> instances;

    private AggregateString(Map<String, Integer> values, Set<InstanceKey> instances) {
        this.values = values;
        this.instances = instances;
    }

    private static AggregateString EMPTY = new AggregateString(Collections.emptyMap(), Collections.emptySet());

    public static AggregateString create() {
        return EMPTY;
    }

    public static AggregateString create(String value, InstanceKey instanceKey) {
        if (instanceKey == null) {
            throw new NullPointerException("AggregateString can not have null InstanceKey. Value -> " + value);
        }
        return new AggregateString(Collections.singletonMap(value, 1), Collections.singleton(instanceKey));
    }

    /**
     * Update a value for an instance.
     * <p>
     * To completely remove a value and its instance, pass in null for newValue
     * 
     * @param oldValue
     * @param newValue
     * @param instanceKey
     * @return
     */
    public AggregateString update(String oldValue, String newValue, InstanceKey instanceKey) {
        if (instanceKey == null) {
            throw new NullPointerException("AggregateString can not have null InstanceKey. Value -> " + newValue);
        }
        boolean containsInstance = instances.contains(instanceKey);
        boolean valuesEqual = valuesEqual(oldValue, newValue);
        if (containsInstance && valuesEqual) {
            // no change
            return this;
        } else {
            Set<InstanceKey> _instances;
            if (containsInstance && newValue != null) {
                _instances = instances; // pass thru
            } else if (containsInstance && newValue == null) {
                _instances = new HashSet<InstanceKey>(instances); // clone
                _instances.remove(instanceKey);
            } else {
                _instances = new HashSet<InstanceKey>(instances); // clone
                _instances.add(instanceKey);
            }

            Map<String, Integer> _values;
            if (valuesEqual) {
                _values = values; // pass thru
            } else {
                _values = new TreeMap<String, Integer>(values); // clone
                if (oldValue != null) {
                    _values.computeIfPresent(oldValue, (key, old) -> {
                        if (old == 1) {
                            return null; // remove
                        } else {
                            return old - 1;
                        }
                    });
                }
                if (newValue != null) {
                    _values.merge(newValue, 1, (e, v) -> {
                        if (e == null) {
                            return v;
                        } else {
                            return e + v;
                        }
                    });
                }
            }

            return new AggregateString(_values, _instances);
        }
    }

    private boolean valuesEqual(String newValue, String oldValue) {
        if (newValue == oldValue)
            return true;
        if (newValue == null) {
            if (oldValue != null)
                return false;
        } else if (!newValue.equals(oldValue)) {
            return false;
        }
        return true;
    }

    public Map<String, Integer> values() {
        return Collections.unmodifiableMap(values);
    }

    public Set<InstanceKey> instances() {
        return Collections.unmodifiableSet(instances);
    }

    public String toJson() {
        return JsonUtility.mapToJson(values);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " => " + toJson();
    }

}