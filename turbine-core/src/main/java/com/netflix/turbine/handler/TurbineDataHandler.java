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
package com.netflix.turbine.handler;

import java.util.Collection;

import com.netflix.turbine.data.TurbineData;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.monitor.TurbineDataMonitor;

/**
 * EventHandler interface for listeners to {@link TurbineData} being emitted from a {@link TurbineDataMonitor}
 */
public interface TurbineDataHandler<K extends TurbineData> {

    /**
     * Name for identification purposes
     * 
     * @return String
     */
    public String getName(); 
    
    /**
     * Receive a collection of data from an turbine data monitor
     * @param stats
     */
    public void handleData(Collection<K> stats);

    /**
     * Notification that a data monitor has gone away or stopped monitoring. 
     * <p>e.g when an InstanceMonitor shuts down due to a connection reset or when a aggregator cluster monitor has been stopped.
     * @param host
     */
    public void handleHostLost(Instance host);
    
    /**
     * Indicate how big a buffer and how many threads are required to dispatch data reliably to this handler
     * @return PerformanceCriteria
     */
    public PerformanceCriteria getCriteria();
}
