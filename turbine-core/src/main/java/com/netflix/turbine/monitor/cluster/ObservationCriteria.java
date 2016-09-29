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

import static org.junit.Assert.*;

import org.junit.Test;

import com.netflix.turbine.discovery.Instance;

/**
 * Class that encapsulates criteria necessary to decide whether to monitor an {@link Instance}
 */
public interface ObservationCriteria {
    
    /**
     * Used for identification purposes
     * @return String
     */
    public String getName();

    /**
     * Decide whether you need stats from this host
     * @param host
     * @return true/false
     */
    public boolean observeHost(Instance host);
    
    /**
     * Simple class that decides to monitor a host based on the specified cluster name
     */
    public static class ClusterBasedObservationCriteria implements ObservationCriteria {
        
        private final String clusterName; 
        
        public ClusterBasedObservationCriteria(String cluster) {
            this.clusterName = cluster;
        }
        
        @Override
        public String getName() {
            return clusterName;
        }

        @Override
        public boolean observeHost(Instance host) {
            if (clusterName == null) {
                return false;
            }
            return clusterName.equals(host.getCluster());
        }
        
        public static class UnitTest {
           
            @Test
            public void testObserve() throws Exception {
                
                ClusterBasedObservationCriteria criteria = new ClusterBasedObservationCriteria("cluster1");
                
                Instance host1 = new Instance("host", "cluster1", true);
                assertTrue(criteria.observeHost(host1));
                
                Instance host2 = new Instance("host", "cluster2", true);
                assertFalse(criteria.observeHost(host2));
                
                Instance host3 = new Instance("host", null, true);
                assertFalse(criteria.observeHost(host3));
                
                ClusterBasedObservationCriteria criteria2 = new ClusterBasedObservationCriteria(null);
                assertFalse(criteria2.observeHost(host1));
                assertFalse(criteria2.observeHost(host2));
                assertFalse(criteria2.observeHost(host3));
            }
        }
    }
}