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
package com.netflix.turbine.monitor.cluster;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.turbine.data.AggDataFromCluster;
import com.netflix.turbine.data.DataFromSingleInstance;
import com.netflix.turbine.data.TurbineData;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.handler.PerformanceCriteria;
import com.netflix.turbine.handler.TurbineDataDispatcher;
import com.netflix.turbine.handler.TurbineDataHandler;
import com.netflix.turbine.monitor.MonitorConsole;
import com.netflix.turbine.monitor.TurbineDataMonitor;
import com.netflix.turbine.monitor.instance.InstanceUrlClosure;
import com.netflix.turbine.monitor.instance.InstanceMonitor;
import com.netflix.turbine.utils.EventThrottle;

/**
 * An implementation of the {@link ClusterMonitor} class that aggregates data from multiple {@link InstanceMonitor} objects for all hosts in the same cluster. 
 * <p>It provides it's own {@link ObservationCriteria} which decides what hosts to monitor. The criteria looks for all the hosts matching the same cluster name.
 * 
 *  @see InstanceMonitor
 */
public class AggregateClusterMonitor extends ClusterMonitor<AggDataFromCluster> {

    private static final Logger logger = LoggerFactory.getLogger(AggregateClusterMonitor.class);
    
    private final ConcurrentHashMap<TurbineData.Key, AggDataFromCluster> aggregateData = new ConcurrentHashMap<TurbineData.Key, AggDataFromCluster>();
    private final TurbineDataHandler<DataFromSingleInstance> eventHandler; 
    
    private final ObservationCriteria observationCriteria;
    private final PerformanceCriteria performanceCriteria;
    
    // cache used to keep track of hosts that have been purged, so that it can ignore any latent data (in the queue) from these hosts
    private final TimeBoundCache<String> timedCache;
    
    public static MonitorConsole<AggDataFromCluster> AggregatorClusterMonitorConsole = new MonitorConsole<AggDataFromCluster>();
    public static MonitorConsole<DataFromSingleInstance> AggregatorInstanceMonitorConsole = new MonitorConsole<DataFromSingleInstance>();
    
    public static TurbineDataDispatcher<DataFromSingleInstance> InstanceMonitorDispatcher = new TurbineDataDispatcher<DataFromSingleInstance>("ALL_INSTANCE_MONITOR_DISPATCHER");
    
    public static TurbineDataMonitor<AggDataFromCluster> findOrRegisterAggregateMonitor(String clusterName) {
        
        TurbineDataMonitor<AggDataFromCluster> clusterMonitor = AggregatorClusterMonitorConsole.findMonitor(clusterName + "_agg");
    
        if (clusterMonitor == null) {
            logger.info("Could not find monitors: " + AggregatorClusterMonitorConsole.toString());
            clusterMonitor = new AggregateClusterMonitor(clusterName + "_agg", clusterName);
            clusterMonitor = AggregatorClusterMonitorConsole.findOrRegisterMonitor(clusterMonitor);
        }
        
        return clusterMonitor;
    }
    
    public AggregateClusterMonitor(String name, String clusterName) {
        this(name, 
             new ObservationCriteria.ClusterBasedObservationCriteria(clusterName), 
             new PerformanceCriteria.AggClusterPerformanceCriteria(clusterName), 
             AggregatorInstanceMonitorConsole, 
             InstanceMonitorDispatcher, 
             InstanceUrlClosure.ClusterConfigBasedUrlClosure);
    }

    public AggregateClusterMonitor(String name,                          // the name of the cluster from which we are aggregating data 
                                   ObservationCriteria observeCriteria,  // how to select hosts that match this cluster when getting host updates from the InstanceObservable
                                   PerformanceCriteria perfCriteria,     // what queue size, num threads et all to use for aggregating events from the InstanceMonitor  
                                   MonitorConsole<DataFromSingleInstance> instanceMonitorConsole,   // where to manage InstanceMonitor(s)
                                   TurbineDataDispatcher<DataFromSingleInstance> instanceMonitorDispatcher, // the instance dispatcher to receive instance monitor events from
                                   InstanceUrlClosure urlClosure) {
        super(name, 
              new TurbineDataDispatcher<AggDataFromCluster>("AGG_CLUSTER_MONITOR_" + name), AggregatorClusterMonitorConsole,
              instanceMonitorDispatcher, instanceMonitorConsole,
              urlClosure);
        
        this.eventHandler = new AggStatsEventHandler(this);
        this.observationCriteria = observeCriteria;
        this.performanceCriteria = perfCriteria;
        
        this.timedCache = new TimeBoundCache<String>(name); // keep state around for 10 mins when hosts are being purged
    }

    @Override
    public void startMonitor() throws Exception {
        super.startMonitor();
        this.timedCache.startCache();
    }

    @Override
    public void stopMonitor() {
        super.stopMonitor();
        this.timedCache.stopCache();
    }
    
    @Override
    public TurbineDataHandler<DataFromSingleInstance> getEventHandler() {
        return eventHandler;
    }

    @Override
    public ObservationCriteria getObservationCriteria() {
        return observationCriteria;
    }

    private boolean stopped() {
        return stopped;
    }
    
    private class AggStatsEventHandler implements TurbineDataHandler<DataFromSingleInstance> {
        
        private AggregateClusterMonitor monitor;
        
        private AggStatsEventHandler(AggregateClusterMonitor monitor) {
            this.monitor = monitor;
        }
        
        final DynamicIntProperty eventFlushThreshold = DynamicPropertyFactory.getInstance().getIntProperty("turbine.aggregator.throttle.eventFlushThreshold", 100);
        final DynamicIntProperty eventFlushDelayMillis = DynamicPropertyFactory.getInstance().getIntProperty("turbine.aggregator.throttle.eventFlushDelay", 3000);
        
        final EventThrottle<DataFromSingleInstance> throttleCheck = new EventThrottle<DataFromSingleInstance>(eventFlushThreshold, eventFlushDelayMillis);
        
        @Override
        public String getName() {
            return monitor.getName() + "_aggClusterEventHandler";
        }
        
        /**
         * Handle new data from the API server we are monitoring.
         */
        @Override
        public void handleData(Collection<DataFromSingleInstance> statsData) {
            
            if(stopped()) {
                return;
            }
            
            for (DataFromSingleInstance data : statsData) {
                
                if(timedCache.lookup(data.getHost().getHostname())) {
                    // this host was removed recently. Pruning out the garbage that is still in handler tuple queue for this host
                    continue;
                }
                
                TurbineData.Key dataKey = data.getKey();
                
                if (logger.isDebugEnabled()) {
                    if (data.getNumericAttributes() != null && data.getNumericAttributes().containsKey("currentTime")) {
                        long timeFromHost = data.getNumericAttributes().get("currentTime");
                        logger.debug("ClusterMonitor data from SingleInstance => Latency: " + (System.currentTimeMillis() - timeFromHost) + "  for [" + data.getName() + "] from " + data.getHost().getHostname());
                    }
                }
                
                // aggregate data
                AggDataFromCluster clusterData = aggregateData.get(dataKey);
                if (clusterData == null) {
                    aggregateData.putIfAbsent(dataKey, new AggDataFromCluster(monitor, data.getType(), data.getName()));
                }
                // count on putIfAbsent to ensure thread-safety, we now just retrieve it again after it's been created by this or another thread
                clusterData = aggregateData.get(dataKey);
                // add this single host data to the cluster data
                clusterData.addStatsDataFromSingleServer(data);
            }
            
            if(throttleCheck.throttle(statsData)) {
                // do nothing
            } else {
                performPostProcessing();
                boolean continueRunning = clusterDispatcher.pushData(getStatsInstance(), aggregateData.values());
                if(!continueRunning) {
                    logger.info("No more listeners to the cluster monitor, stopping monitor");
                    monitor.stopMonitor();   // calling stop on the enclosing monitor
                }
            }
        }

        private void performPostProcessing() {
            for (AggDataFromCluster data : aggregateData.values()) {
                data.performPostProcessing();
            }
        }
        
        /**
         * Handle an APIInstance being lost (we lost the connection somehow, perhaps it was shutdown)
         */
        @Override
        public void handleHostLost(Instance host) {
            logger.info("Host lost: " + host.getHostname() + ", adding to time based cache\n");
            timedCache.put(host.getHostname());
            
            for (TurbineData.Key key : aggregateData.keySet()) {
                AggDataFromCluster dataFromCluster = aggregateData.get(key);
                dataFromCluster.removeDataForHost(host);
            }
        }

        @Override
        public PerformanceCriteria getCriteria() {
            return performanceCriteria;
        }
        
        @Override
        public String toString() {
            return this.getName();
        }
    }
}
