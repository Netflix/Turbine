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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.netflix.turbine.monitor.cluster.ObservationCriteria;

/**
 * Class that encapsulates a host or machine that is vending data to Turbine.
 *  
 * <p>Turbine uses the notion of <b>cluster</b> to group Instances together for agggregation. 
 * 
 * <p>Turbine also treats the host status as a first class citizen where the status conveys whether the 
 * host is ready to serve data etc. 
 * <ul>
 * <li>isUp = true means that Turbine must connect to the host
 * <li>isUp = false indicates that Turbine should either not connect or disconnect if it is already connected. 
 * </ul>
 * <p>Once Turbine disconnects, all the relevant data from that host is purged from it's aggregated state.  
 * 
 * <p> You can also use the attrs to add in free form details about an Instance. 
 * e.g ec2-availability-zone, build revision etc. 
 * <p> Then you can also write your own {@link ObservationCriteria} implementation to decide how to filter out
 * relevant hosts to aggregate data from.
 * 
 */
public class Instance implements Comparable<Instance> {

    private final String hostname; 
    private final String cluster;
    private final boolean isUp;
    private final Map<String, String> attributes; 
    
    /**
     * @param host
     * @param clusterName
     * @param status
     */
    public Instance(String host, String clusterName, boolean status) {
        hostname = host;
        cluster = clusterName;
        isUp = status;
        attributes = new HashMap<String, String>();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        
        Instance other = (Instance) obj;
        
        boolean equals = true; 
        equals &= (this.hostname != null) ? (this.hostname.equals(other.hostname)) : (other.hostname == null);
        equals &= (this.cluster != null) ? (this.cluster.equals(other.cluster)) : (other.cluster == null);
        equals &= (this.isUp == other.isUp);

        return equals;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hostname == null) ? 0 : hostname.hashCode());
        result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
        result = prime * result + (isUp ? 1 : 0);
        return result;
    }
    
    @Override
    public String toString() {
        return "StatsInstance [hostname=" + hostname + ", cluster: " + cluster + ", isUp: " + isUp + ", attrs=" + attributes.toString() + "]";
    }

    /**
     * @return String
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * @return String
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * @return true/false
     */
    public boolean isUp() {
        return isUp;
    }

    /**
     * @return Map<String, String>
     */
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public int compareTo(Instance other) {
        return this.getHostname().compareTo(other.getHostname());
    }

    public static class UnitTest {
        
        @Test
        public void testHostEquals() throws Exception {
            
            Instance host1 = new Instance("h1", "c1", true);
            Instance host2 = new Instance("h1", "c2", false);
            Instance host3 = new Instance("h2", "c1", true);
            
            assertFalse(host1.equals(host2));
            assertFalse(host1.equals(host3));
            assertFalse(host2.equals(host3));
        }
    }
}
