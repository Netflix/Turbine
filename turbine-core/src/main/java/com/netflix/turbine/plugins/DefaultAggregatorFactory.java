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
package com.netflix.turbine.plugins;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.turbine.data.AggDataFromCluster;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.discovery.InstanceDiscovery;
import com.netflix.turbine.handler.PerformanceCriteria;
import com.netflix.turbine.handler.TurbineDataHandler;
import com.netflix.turbine.monitor.TurbineDataMonitor;
import com.netflix.turbine.monitor.cluster.AggregateClusterMonitor;
import com.netflix.turbine.monitor.cluster.ClusterMonitor;
import com.netflix.turbine.monitor.cluster.ClusterMonitorFactory;

/**
 * Default plugin for using the {@link AggregateClusterMonitor} on startup.
 * <p>Note that the factory requires a comma separated list of cluster names in order to start the individual cluster monitor for each cluster.
 * <p>The property is <b>turbine.aggregator.clusterConfig</b>
 * <p>e.g config
 * <br>
 * turbine.aggregator.clusterConfig=prod,prod-backup
 *
 * <p> Note that since the aggregator shuts down when no-one is listening we just attach a bogus no-op {@link TurbineDataHandler}
 * here to keep the aggergator running. This helps in getting the data really fast when a real listener comes along, since the
 * aggregator will then already have all the connections set up and data will be flowing from the multiple server instances.
 *
 */
public class DefaultAggregatorFactory implements ClusterMonitorFactory<AggDataFromCluster> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAggregatorFactory.class);

    // config
    private static final DynamicStringProperty aggClusters = DynamicPropertyFactory.getInstance().getStringProperty(InstanceDiscovery.TURBINE_AGGREGATOR_CLUSTER_CONFIG, null);

    /**
     * @return {@link ClusterMonitor}<{@link AggDataFromCluster}>
     */
    @Override
    public ClusterMonitor<AggDataFromCluster> getClusterMonitor(String name) {
        TurbineDataMonitor<AggDataFromCluster> clusterMonitor = AggregateClusterMonitor.AggregatorClusterMonitorConsole.findMonitor(name + "_agg");
        return (ClusterMonitor<AggDataFromCluster>) clusterMonitor;
    }

    /**
     * Inits all configured cluster monitors
     */
    @Override
    public void initClusterMonitors() {

        for(String clusterName : getClusterNames()) {
            ClusterMonitor<AggDataFromCluster> clusterMonitor = (ClusterMonitor<AggDataFromCluster>) AggregateClusterMonitor.findOrRegisterAggregateMonitor(clusterName);
            clusterMonitor.registerListenertoClusterMonitor(StaticListener);
            try {
                clusterMonitor.startMonitor();
            } catch (Exception e) {
                logger.warn("Could not init cluster monitor for: " + clusterName);
                clusterMonitor.stopMonitor();
                clusterMonitor.getDispatcher().stopDispatcher();
            }
        }
    }

    /**
     * shutdown all configured cluster monitors
     */
    @Override
    public void shutdownClusterMonitors() {

        for(String clusterName : getClusterNames()) {
            ClusterMonitor<AggDataFromCluster> clusterMonitor = (ClusterMonitor<AggDataFromCluster>) AggregateClusterMonitor.findOrRegisterAggregateMonitor(clusterName);
            clusterMonitor.stopMonitor();
            clusterMonitor.getDispatcher().stopDispatcher();
        }
    }

    private List<String> getClusterNames() {

        List<String> clusters = new ArrayList<String>();
        String clusterNames = aggClusters.get();
        if (clusterNames == null || clusterNames.trim().length() == 0) {
            clusters.add("default");
        } else {
            String[] parts = aggClusters.get().split(",");
            for (String s : parts) {
                clusters.add(s);
            }
        }
        return clusters;
    }
    private TurbineDataHandler<AggDataFromCluster> StaticListener = new TurbineDataHandler<AggDataFromCluster>() {

        @Override
        public String getName() {
            return "StaticListener_For_Aggregator";
        }

        @Override
        public void handleData(Collection<AggDataFromCluster> stats) {
        }

        @Override
        public void handleHostLost(Instance host) {
        }

        @Override
        public PerformanceCriteria getCriteria() {
            return NonCriticalCriteria;
        }

    };

    private PerformanceCriteria NonCriticalCriteria = new PerformanceCriteria() {

        @Override
        public boolean isCritical() {
            return false;
        }

        @Override
        public int getMaxQueueSize() {
            return 0;
        }

        @Override
        public int numThreads() {
            return 0;
        }
    };
}
