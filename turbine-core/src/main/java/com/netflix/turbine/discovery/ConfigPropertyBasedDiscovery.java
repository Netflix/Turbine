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
package com.netflix.turbine.discovery;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;

public class ConfigPropertyBasedDiscovery implements InstanceDiscovery {
    private static final DynamicStringProperty DefaultClusterInstanceList = DynamicPropertyFactory.getInstance().getStringProperty("turbine.ConfigPropertyBasedDiscovery.default.instances", null);
    
    private static final DynamicStringProperty ClusterList = DynamicPropertyFactory.getInstance().getStringProperty(TURBINE_AGGREGATOR_CLUSTER_CONFIG, null);
    
    @Override
    public Collection<Instance> getInstanceList() throws Exception {
        
        List<String> clusters = getClusterList();
        List<Instance> instances = new ArrayList<Instance>();
        for (String cluster : clusters) {
            instances.addAll(getInstancesForCluster(cluster));
        }
        return instances;
    }
    
    private List<String> getClusterList() throws Exception {
        
        String clusterConfig = ClusterList.get(); 
        if (clusterConfig == null || clusterConfig.trim().length() == 0) {
            // check if there is a list of default instances. If yes, then use 'default' as the cluster
            String defaultInstannceList = DefaultClusterInstanceList.get(); 
            if (defaultInstannceList == null || defaultInstannceList.trim().length() == 0) {
                throw new Exception("Must configure instance list using property: " + DefaultClusterInstanceList.getName());
            }
            // return cluster as default
            return Lists.newArrayList("default");
        }
        // cluster config is not null. Parse cluster config.
        List<String> clusters = Lists.newArrayList(clusterConfig.trim().split(","));
        
        if (clusters.size() == 0) {
            throw new Exception("Must configure property: " + ClusterList.getName());
        }
        return clusters;
    }
    
    private List<Instance> getInstancesForCluster(String cluster) throws Exception {
        
        DynamicStringProperty instanceListProp = DynamicPropertyFactory.getInstance().getStringProperty("turbine.ConfigPropertyBasedDiscovery." + cluster + ".instances", null);
        String instanceList = instanceListProp.get();
        if (instanceList == null || instanceList.trim().length() == 0) {
            throw new Exception("Must configure Instance list property: " + instanceListProp.getName());
        }

        String[] parts = instanceList.split(",");

        List<Instance> instances = new ArrayList<Instance>();
        for (String s : parts) {
            instances.add(new Instance(s, cluster, true));
        }
        return instances;
    }
    
    public static class UnitTest {
        
        @Test(expected=Exception.class)
        public void testNothingConfigured() throws Exception {
            
            new ConfigPropertyBasedDiscovery().getInstanceList();
        }

        @Test(expected=Exception.class)
        public void testWrongClusterConfigured() throws Exception {
            
            ConfigurationManager.getConfigInstance().setProperty(TURBINE_AGGREGATOR_CLUSTER_CONFIG, "test");
            try {
                new ConfigPropertyBasedDiscovery().getInstanceList();
            } finally {
                ConfigurationManager.getConfigInstance().setProperty(TURBINE_AGGREGATOR_CLUSTER_CONFIG, "");
            }
        }
        
        @Test
        public void testUsingDefaults() throws Exception {
            
            ConfigurationManager.getConfigInstance().setProperty("turbine.ConfigPropertyBasedDiscovery.default.instances", "foo,bar");
            List<Instance> instances = (List<Instance>) new ConfigPropertyBasedDiscovery().getInstanceList();
            
            assertEquals("foo", instances.get(0).getHostname());
            assertEquals("default", instances.get(0).getCluster());
            assertEquals("bar", instances.get(1).getHostname());
            assertEquals("default", instances.get(1).getCluster());
            ConfigurationManager.getConfigInstance().setProperty("turbine.ConfigPropertyBasedDiscovery.default.instances", "");
        }

        @Test
        public void testUsingConfiguredCluster() throws Exception {
            
            ConfigurationManager.getConfigInstance().setProperty(TURBINE_AGGREGATOR_CLUSTER_CONFIG, "test");
            ConfigurationManager.getConfigInstance().setProperty("turbine.ConfigPropertyBasedDiscovery.test.instances", "foo1,bar1");

            List<Instance> instances = (List<Instance>) new ConfigPropertyBasedDiscovery().getInstanceList();
            
            assertEquals("foo1", instances.get(0).getHostname());
            assertEquals("test", instances.get(0).getCluster());
            assertEquals("bar1", instances.get(1).getHostname());
            assertEquals("test", instances.get(1).getCluster());
            ConfigurationManager.getConfigInstance().setProperty(TURBINE_AGGREGATOR_CLUSTER_CONFIG, "");
            ConfigurationManager.getConfigInstance().setProperty("turbine.ConfigPropertyBasedDiscovery.default.instances", "");
        }
        
    }
}
