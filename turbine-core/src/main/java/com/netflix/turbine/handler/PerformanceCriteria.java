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

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.turbine.monitor.cluster.AggregateClusterMonitor;
import com.netflix.turbine.monitor.instance.InstanceMonitor;


/**
 * Interface that encapsulates the perf criteria expressed by a {@link TurbineDataHandler} 
 * <p>Note that data is generally dispatched to {@link TurbineDataHandler} using multiple threads and 
 * the <b>queue size</b> and <b>num threads</b> can help determine the throughput of a handler. 
 * 
 */
public interface PerformanceCriteria { 
    
    /**
     * Do we care about this handler's performance? Sometimes we use dummy handlers e.g STATIC LISTENER
     * There are 2 main arguments here  
     *    1. Queue Size - how many elements should be queued before the next elements start being discarded. This is related to latency of the data
     *    2. How many threads are allowed to pull data simultaneously off the queue and dispatch to the handler in parallel
     * @return boolean
     */
    public boolean isCritical();
    
    /**
     * What is the max no of elements that the handler can accept as queued before they are delivered to the handler
     * @return int
     */
    public int getMaxQueueSize();
    
    /**
     * What is the no of threads that the handler would like to have data sent from. 
     * @return int
     */
    public int numThreads();
    
    /**
     * Sample criteria that is used by the {@link AggregateClusterMonitor#getEventHandler()} to handle data from multiple {@link InstanceMonitor} connections.
     * <p>Note that one can configure the perf size for each cluster that is being run by Turbine using the properties 
     * <b>turbine.aggCluster.performance.queueSize.[clusterName]</b>  and <b>turbine.aggCluster.performance.numThreads.[clusterName]</b>
     *
     */
    public static class AggClusterPerformanceCriteria implements PerformanceCriteria {

        private final DynamicBooleanProperty isCritical; 
        private final DynamicIntProperty queueSize; 
        private final DynamicIntProperty numThreads; 

        public AggClusterPerformanceCriteria(String clusterName) {
            isCritical = DynamicPropertyFactory.getInstance().getBooleanProperty("turbine.aggCluster.performance.isCritical." + clusterName, true);
            queueSize = DynamicPropertyFactory.getInstance().getIntProperty("turbine.aggCluster.performance.queueSize." + clusterName, 10000);
            numThreads = DynamicPropertyFactory.getInstance().getIntProperty("turbine.aggCluster.performance.numThreads." + clusterName, 1);
        }
        @Override
        public boolean isCritical() {
            return isCritical.get();
        }

        @Override
        public int getMaxQueueSize() {
            return queueSize.get();
        }

        @Override
        public int numThreads() {
            return numThreads.get();
        }
    }
}
