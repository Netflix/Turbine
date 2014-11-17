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

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;

import org.junit.Test;

import com.netflix.turbine.aggregator.NumberList;

public class NumberListTest {

    private static final LinkedHashMap<String, Object> values = new LinkedHashMap<String, Object>();
    private static final LinkedHashMap<String, Object> values2 = new LinkedHashMap<String, Object>();
    static {
        values.put("0", 10);
        values.put("25", 50);
        values.put("50", 100);
        values.put("75", 250);
        values.put("99", 500);

        values2.put("0", 15);
        values2.put("25", 55);
        values2.put("50", 105);
        values2.put("75", 255);
        values2.put("99", 505);
    }

    @Test
    public void testCreate() {
        NumberList nl = NumberList.create(values);
        System.out.println(nl);
        assertEquals(Long.valueOf(10), nl.get("0"));
        assertEquals(Long.valueOf(50), nl.get("25"));
        assertEquals(Long.valueOf(100), nl.get("50"));
        assertEquals(Long.valueOf(250), nl.get("75"));
        assertEquals(Long.valueOf(500), nl.get("99"));
    }

    @Test
    public void testDelta() {
        NumberList nl = NumberList.delta(values2, values);
        System.out.println(nl);
        assertEquals(Long.valueOf(5), nl.get("0"));
        assertEquals(Long.valueOf(5), nl.get("25"));
        assertEquals(Long.valueOf(5), nl.get("50"));
        assertEquals(Long.valueOf(5), nl.get("75"));
        assertEquals(Long.valueOf(5), nl.get("99"));
    }
    
    @Test
    public void testDeltaWithNumberList() {
        NumberList nl = NumberList.delta(NumberList.create(values2), NumberList.create(values));
        System.out.println(nl);
        assertEquals(Long.valueOf(5), nl.get("0"));
        assertEquals(Long.valueOf(5), nl.get("25"));
        assertEquals(Long.valueOf(5), nl.get("50"));
        assertEquals(Long.valueOf(5), nl.get("75"));
        assertEquals(Long.valueOf(5), nl.get("99"));
    }

    @Test
    public void testSum() {
        NumberList nl = NumberList.create(values).sum(NumberList.create(values2));
        System.out.println(nl);
        assertEquals(Long.valueOf(25), nl.get("0"));
        assertEquals(Long.valueOf(105), nl.get("25"));
        assertEquals(Long.valueOf(205), nl.get("50"));
        assertEquals(Long.valueOf(505), nl.get("75"));
        assertEquals(Long.valueOf(1005), nl.get("99"));
    }
}
