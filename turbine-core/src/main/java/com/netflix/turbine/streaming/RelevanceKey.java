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
package com.netflix.turbine.streaming;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;

import org.junit.Test;

/**
 * Decides the sort order of items in a list based on the {@link RelevanceConfig} provided
 */
public class RelevanceKey {

    private final String name; 
    private final List<RelevanceItem> items; 
    private Long relevance = 0L;
    
    /**
     * @param n    : topN spec
     * @param it   : list of config criteria
     * @param data : the underlying data to calculate the cost for
     */
    public RelevanceKey(String n, List<RelevanceItem> it, Map<String, Long> data) {
        name = n;
        items = it;
        computeRelevance(data);
    }
    
    public String getName() {
        return name;
    }
    
    public Long getRelevance() {
        return relevance;
    }
    
    private void computeRelevance(Map<String, Long> data) {
        double dRelevance = 0L;
        for (RelevanceItem item : items) {
            Long value = data.get(item.attrKey);
            dRelevance += item.getRelevance(value);
            relevance = Math.round(dRelevance);
        }
    }
    
    @Override
    public String toString() { 
        return name + ": " + relevance;
    }
    
    public static class RelevanceItem {
        
        public final String attrKey; 
        public final double weight; 
        
        public RelevanceItem(String attrKey, double weight) {
            this.attrKey = attrKey;
            this.weight = weight;
        }

        public double getRelevance(Long value) {
            if (value == null) {
                return -1L;
            }
            return weight*value;
        }

        @Override
        public String toString() {
            return " " + attrKey + ":" + weight;
        }
    }
    
    public static class RelevanceComparator implements Comparator<RelevanceKey> {

        @Override
        public int compare(RelevanceKey arg0, RelevanceKey arg1) {
            
            int compareName = arg0.getName().compareTo(arg1.getName());
            int compareRelevance = arg0.getRelevance().compareTo(arg1.getRelevance());

            // In case the relevance is the same, then disambiguate using the name
            if (compareRelevance == 0) {
                return compareName;
            } else {
                return compareRelevance;
            }
        }
    }
    
    public static class UnitTest {
        
        @Test
        public void testSortOnErrorPercentage() throws Exception {
         
            List<RelevanceItem> items = new ArrayList<RelevanceItem>();
            items.add(new RelevanceItem("errorPercentage", 100));

            Map<String, Long> attrs1 = new HashMap<String, Long>();
            attrs1.put("errorPercentage", 3L);
            
            RelevanceKey key1 = new RelevanceKey("AB", items, attrs1);

            Map<String, Long> attrs2 = new HashMap<String, Long>();
            attrs2.put("errorPercentage", 3L);
            
            RelevanceKey key2 = new RelevanceKey("Cinematch", items, attrs2);
            
            Map<String, Long> attrs3 = new HashMap<String, Long>();
            attrs3.put("errorPercentage", 1L);
            
            RelevanceKey key3 = new RelevanceKey("Subscriber", items, attrs3);

            ConcurrentSkipListSet<RelevanceKey> set = new ConcurrentSkipListSet<RelevanceKey>(new RelevanceComparator());
            
            set.add(key1);
            set.add(key2);
            set.add(key3);
            
            Iterator<RelevanceKey> iter = set.iterator();
            assertEquals("Subscriber", iter.next().name);
            assertEquals("AB", iter.next().name);
            assertEquals("Cinematch", iter.next().name);
        }

        @Test
        public void testSortOnErrorAndVolume() throws Exception {
         
            List<RelevanceItem> items = new ArrayList<RelevanceItem>();
            items.add(new RelevanceItem("errorPercentage", 100));
            items.add(new RelevanceItem("volume", 1));

            Map<String, Long> attrs1 = new HashMap<String, Long>();
            attrs1.put("errorPercentage", 3L);
            attrs1.put("volume", 100L);
            
            RelevanceKey key1 = new RelevanceKey("AB", items, attrs1);

            Map<String, Long> attrs2 = new HashMap<String, Long>();
            attrs2.put("errorPercentage", 3L);
            attrs2.put("volume", 0L);
            
            RelevanceKey key2 = new RelevanceKey("Cinematch", items, attrs2);
            
            Map<String, Long> attrs3 = new HashMap<String, Long>();
            attrs3.put("errorPercentage", 1L);
            attrs3.put("volume", 200L);
            
            RelevanceKey key3 = new RelevanceKey("Subscriber", items, attrs3);

            ConcurrentSkipListSet<RelevanceKey> set = new ConcurrentSkipListSet<RelevanceKey>(new RelevanceComparator());
            
            set.add(key1);
            set.add(key2);
            set.add(key3);
            
            Iterator<RelevanceKey> iter = set.iterator();
            assertEquals("Cinematch", iter.next().name);
            assertEquals("Subscriber", iter.next().name);
            assertEquals("AB", iter.next().name);
        }
        
    }
}
