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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.netflix.turbine.streaming.RelevanceKey.RelevanceItem;

/**
 * Class that encapsulates the config required for specifying the relevance of data in a data stream. 
 * You can specify weights for the attributes of the data in order to dictate it's sort order. 
 * 
 * <p>e.g 
 * <p>RelevanceConfig can be specified as
 * <p> cost = volume x 1.0  + error x 100.0
 * 
 */
public class RelevanceConfig {
    
    public final String name; 
    public final String type; 
    public final List<RelevanceItem> items; 
    public final int topN;
    
    public RelevanceConfig(String name, String type, int topN, Map<String, Double> sort) {
        
        this.name = name;
        this.type = type;
        this.topN = topN;

        items = new ArrayList<RelevanceItem>();
        for (String s : sort.keySet()) {
            items.add(new RelevanceItem(s, sort.get(s)));
        }
    }
    
    @Override
    public String toString() {
        return "name:" + name + ", type:" + type + ", topN:" + topN + ", config:" + items.toString();
    }
}
