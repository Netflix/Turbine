/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.turbine.data;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.netflix.turbine.monitor.TurbineDataMonitor;

/**
 * Base class for storing, passing around and delivering data from remote hosts in a JSON format ready for clients.
 * See {@link DataFromSingleInstance} and {@link AggDataFromCluster} for concrete implementations
 * 
 * Note that Turbine needs an aggregation dimension when combining data from multiple {@link DataFromSingleInstance} objects
 * into a single  {@link AggDataFromCluster} object.
 * The <b>name</b> and <b>type</b> are used for this purpose. name + type is the unique aggregation key. 
 * 
 * <p>
 * <b>name</b> identifies a single data point / event / metric etc whereas <b>type</b> is used to denote the sub stream type 
 * which then allows us to multiplex multiple sub streams over the same connection from an instance. 
 * 
 * <p> <b>name</b> and <b>type</b> can also be specified as filtering criteria when fetching the output stream from the 
 * turbine aggregator.
 * 
 */
public abstract class TurbineData {

    // unique key for identifying individual data points
    protected final Key key;
    // the associated monitor for this data
    private final TurbineDataMonitor<?> monitor;
    private long creationTime;

    /**
     * @param monitor
     * @param type
     * @param name
     */
    public TurbineData(TurbineDataMonitor<?> monitor, String type, String name) {
        this.monitor = monitor;
        this.key = new Key(type, name);
        this.creationTime = System.currentTimeMillis();
    }

    /**
     * @return Key
     */
    public Key getKey() {
        return key;
    }

    /**
     * @return String
     */
    public String getType() {
        return key.getType();
    }

    /**
     * @return String
     */
    public String getName() {
        return key.getName();
    }
    
    /**
     * @return long
     */
    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long time) {
        creationTime = time;
    }
    
    /**
     * @return StatsRollingNumber
     */
    public StatsRollingNumber getRolling2MinuteStats() {
        return monitor.getRolling2MinuteStats(this);
    }
    
    /**
     * @return TurbineDataMonitor<?> 
     */
    public TurbineDataMonitor<?> getMonitor() {
        return monitor;
    }
    
    /**
     * @return HashMap<String, Long>
     */
    public abstract HashMap<String, Long> getNumericAttributes();

    /**
     * @return HashMap<String, String>
     */
    public abstract HashMap<String, String> getStringAttributes();

    /**
     * 
     * @return
     */
    public abstract HashMap<String, Map<String, ? extends Number>> getNestedMapAttributes();

    public Map<String, Object> getAttributes() {
        Map<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("type", getType());
        dataMap.put("name", getName());

        // output the numbers
        HashMap<String, Long> numericAttributes = getNumericAttributes();
        if (numericAttributes != null) {
            for (String key : numericAttributes.keySet()) {
                dataMap.put(key, numericAttributes.get(key));
            }
        }

        // output the strings
        HashMap<String, String> stringAttributes = getStringAttributes();
        if (stringAttributes != null) {
            for (String key : stringAttributes.keySet()) {
                String value = stringAttributes.get(key);
                if (value.equals("true") || value.equals("false")) {
                    dataMap.put(key, Boolean.valueOf(value));
                } else {
                    dataMap.put(key, value);
                }
            }
        }

        return dataMap;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "_" + getName();
    }

    /**
     * Type-safe representation of key Data Type+Name
     * 
     */
    public static class Key {
        private final String name;
        private final String type;

        public Key(String type, String name) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return getType() + "_" + getName();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            
            Key other = (Key) obj;
            boolean equals = (name != null) ? name.equals(other.name) : other.name == null;
            equals &= (type != null) ? type.equals(other.type) : other.type == null;
            
            return equals;
        }

    }

    public static class UnitTest {

        @Test
        public void testDefaultJSON() {
            TestStatsData stats = new TestStatsData("DEFAULT", "testDefaultJSON");

            try {
                Map<String, Object> o = stats.getAttributes();
                assertEquals("DEFAULT", o.get("type"));
                assertEquals("testDefaultJSON", o.get("name"));
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        }

        private static class TestStatsData extends DataFromSingleInstance {

            public HashMap<String, Long> numericAttributes = new HashMap<String, Long>();
            public HashMap<String, String> stringAttributes = new HashMap<String, String>();

            public TestStatsData(String type, String name) {
                super(null, type, name, null, new HashMap<String, Object>(), 1);
            }

            @Override
            public HashMap<String, Long> getNumericAttributes() {
                return numericAttributes;
            }

            @Override
            public HashMap<String, String> getStringAttributes() {
                return stringAttributes;
            }
        }
    }
}
