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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.netflix.turbine.internal.JsonUtility;
import com.netflix.turbine.internal.NumberListSerializer;

@JsonSerialize(using = NumberListSerializer.class)
public class NumberList {

    public static NumberList create(Map<String, Object> numbers) {
        LinkedHashMap<String, Long> values = new LinkedHashMap<String, Long>(numbers.size());
        for (Entry<String, Object> k : numbers.entrySet()) {
            Object v = k.getValue();
            if (v instanceof Number) {
                values.put(k.getKey(), ((Number) v).longValue());
            } else {
                values.put(k.getKey(), Long.valueOf(String.valueOf(v)));
            }
        }

        return new NumberList(values);
    }

 // unchecked but we know we can go from Map<String, Long> to Map<String, Object>
    @SuppressWarnings("unchecked")
    public static NumberList delta(Map<String, Object> currentMap, NumberList previousMap) {
        return delta(currentMap, (Map)previousMap.numbers);
    }
    
    // unchecked but we know we can go from Map<String, Long> to Map<String, Object>
    @SuppressWarnings("unchecked")
    public static NumberList delta(NumberList currentMap, NumberList previousMap) {
        return delta((Map)currentMap.numbers, (Map)previousMap.numbers);
    }
    
    /**
     * This assumes both maps contain the same keys. If they don't then keys will be lost.
     * 
     * @param currentMap
     * @param previousMap
     * @return
     */
    public static NumberList delta(Map<String, Object> currentMap, Map<String, Object> previousMap) {
        LinkedHashMap<String, Long> values = new LinkedHashMap<String, Long>(currentMap.size());
        if(currentMap.size() != previousMap.size()) {
            throw new IllegalArgumentException("Maps must have the same keys");
        }
        for (Entry<String, Object> k : currentMap.entrySet()) {
            Object v = k.getValue();
            Number current = getNumber(v);
            Object p = previousMap.get(k.getKey());
            Number previous = null;
            if (p == null) {
                previous = 0;
            } else {
                previous = getNumber(p);
            }

            long d = (current.longValue() - previous.longValue());
            values.put(k.getKey(), d);
        }

        return new NumberList(values);
    }
    
    /**
     * Return a NubmerList with the inverse of the given values.
     */
    public static NumberList deltaInverse(Map<String, Object> map) {
        LinkedHashMap<String, Long> values = new LinkedHashMap<String, Long>(map.size());
        for (Entry<String, Object> k : map.entrySet()) {
            Object v = k.getValue();
            Number current = getNumber(v);
            long d = -current.longValue();
            values.put(k.getKey(), d);
        }

        return new NumberList(values);
    }

    public NumberList sum(NumberList delta) {
        LinkedHashMap<String, Long> values = new LinkedHashMap<String, Long>(numbers.size());
        for (Entry<String, Long> k : numbers.entrySet()) {
            Long p = k.getValue();
            Long d = delta.get(k.getKey());
            Long previous = null;
            if (p == null) {
                previous = 0L;
            } else {
                previous = p;
            }

            // if we're missing the value in the delta we negate it
            if (d == null) {
                d = -previous;
            }
            long sum = d + previous;
            values.put(k.getKey(), sum);
        }

        return new NumberList(values);
    }

    private static Number getNumber(Object v) {
        Number n = null;
        if (v instanceof Number) {
            n = ((Number) v).longValue();
        } else {
            n = Long.valueOf(String.valueOf(v));
        }
        return n;
    }

    public static NumberList empty() {
        return create(Collections.emptyMap());
    }

    private final Map<String, Long> numbers;

    private NumberList(Map<String, Long> numbers) {
        this.numbers = numbers;
    }

    public Map<String, Long> getMap() {
        return numbers;
    }

    public Long get(String key) {
        return numbers.get(key);
    }

    public Set<Entry<String, Long>> getEntries() {
        return numbers.entrySet();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " => " + numbers.entrySet();
    }

    public String toJson() {
        return JsonUtility.mapToJson(numbers);
    }

}
