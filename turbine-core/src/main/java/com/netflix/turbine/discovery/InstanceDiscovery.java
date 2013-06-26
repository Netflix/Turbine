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

import java.util.Collection;

import com.netflix.turbine.plugins.PluginsFactory;

/**
 * Interface that encapsulates a source of {@link Instance} info. Note that Turbine currently uses a 
 * poll mechanism to fetch Instance info at periodic intervals using the {@link InstanceObservable}
 * 
 * <p>The InstanceObservable introspects the {@link Instance#isUp()} state to understand whether to 
 * notify observers of hosts that are up and down. 
 * <p>It also keeps state from the previous poll and it uses this to determine whether some older hosts 
 * have disappeared.
 * 
 * <p>If the {@link InstanceDiscovery} object throws an exception, this is caught and logged by the {@link InstanceObservable}
 *  and the data update is ignored, but this does not stop the InstanceObservable thread. 
 *
 * <p>Note that you can provide your own {@link InstanceDiscovery} using the {@link PluginsFactory}. Turbine also provides
 * a default static file based {@link FileBasedInstanceDiscovery} implementation as a default to help get started.
 */
public interface InstanceDiscovery {
	public static final String TURBINE_AGGREGATOR_CLUSTER_CONFIG = "turbine.aggregator.clusterConfig";
	
    /**
     * Fetch the collection of Instances.  
     * @return Collection<Instance>
     * @throws Exception
     */
    public Collection<Instance> getInstanceList() throws Exception;
}
