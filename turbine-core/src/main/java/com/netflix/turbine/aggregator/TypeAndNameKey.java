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

import java.util.concurrent.ConcurrentHashMap;

public final class TypeAndNameKey implements GroupKey {
    private final String key;
    private final String type;
    private final String name;

    private static ConcurrentHashMap<String, TypeAndNameKey> internedKeys = new ConcurrentHashMap<>();

    public static TypeAndNameKey from(String type, String name) {
        // I wish there was a way to do compound keys without creating new strings
        return internedKeys.computeIfAbsent(type + "_" + name, k -> new TypeAndNameKey(k, type, name));
    }

    private TypeAndNameKey(String key, String type, String name) {
        this.key = key;
        this.type = type;
        this.name = name;
    }

    public String getKey() {
        return key;
    }
    
    public String getType() {
        return type;
    }
    
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "TypeAndName=>" + key;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TypeAndNameKey other = (TypeAndNameKey) obj;
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        return true;
    }

}