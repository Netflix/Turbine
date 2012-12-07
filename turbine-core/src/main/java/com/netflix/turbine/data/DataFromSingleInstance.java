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

import static org.junit.Assert.*;

import java.util.HashMap;

import org.junit.Test;

import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.monitor.TurbineDataMonitor;

/**
 * Data for a given <b>name</b> and <b>type</b> from a single {@link Instance}. 
 * <p>There are numeric attributes and there are string attributes.
 *
 */
public class DataFromSingleInstance extends TurbineData {
    
    // the host that is the source of this data
    private final Instance host;

    private final HashMap<String, Long> numericAttributes;
    private final HashMap<String, String> stringAttributes;

    /**
     * @param monitor - the InstanceMonitor for this host. 
     * @param type 
     * @param name
     * @param host
     * @param attributes
     * @param dateTime
     */
    public DataFromSingleInstance(TurbineDataMonitor<DataFromSingleInstance> monitor, 
                                  String type, 
                                  String name, 
                                  Instance host, 
                                  HashMap<String, Object> attributes, 
                                  long dateTime) {
        super(monitor, type, name);
        this.host = host;
        super.setCreationTime(dateTime);

        numericAttributes = new HashMap<String, Long>();
        stringAttributes = new HashMap<String, String>();
        
        /* populate our 2 internal maps while determining which values are ints */
        for (String key : attributes.keySet()) {
            Object value = attributes.get(key);
            if ((value instanceof Integer || value instanceof Long)) {
                long longValue = Long.parseLong(String.valueOf(value));
                numericAttributes.put(key, longValue);
            } else {
                stringAttributes.put(key, String.valueOf(value));
            }
        }
    }
    
    public DataFromSingleInstance(TurbineDataMonitor<DataFromSingleInstance> monitor, String type, String name, 
            Instance host, HashMap<String, Long> nAttrs, HashMap<String, String> sAttrs, long dataTime) {
        super(monitor, type, name);
        this.host = host;
        super.setCreationTime(dataTime);

        numericAttributes = nAttrs;
        stringAttributes = sAttrs;
    }

    @Override
    public HashMap<String, Long> getNumericAttributes() {
        return numericAttributes;
    }

    @Override
    public HashMap<String, String> getStringAttributes() {
        return stringAttributes;
    }
    
    public Instance getHost() {
        return this.host;
    }
    
    public static class UnitTest { 
        
        @Test
        public void testParseAttributes() throws Exception {
            
            HashMap<String, Object> attrs = new HashMap<String, Object>();
            attrs.put("s1", "v1");
            attrs.put("n1", 1234);
            attrs.put("b1", true);
            attrs.put("b2", false);
            
            Instance host = new Instance("host", "cluster", true);
            
            DataFromSingleInstance data = 
                    new DataFromSingleInstance(null, "type", "name", host, attrs, System.currentTimeMillis());
            
            assertEquals("v1", data.getStringAttributes().get("s1"));
            assertEquals("true", data.getStringAttributes().get("b1"));
            assertEquals("false", data.getStringAttributes().get("b2"));
            assertTrue(1234 == data.getNumericAttributes().get("n1"));
        }
    }
}
