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

import com.netflix.turbine.data.TurbineData;

/**
 * An interface that encapsulated a factory for vending {@link ClusterMonitor} implementations.
 * <p>By default we use the {@link AggregateClusterMonitor} implementation if none other is specified.
 *
 * @param <T>
 */
public interface ClusterMonitorFactory<T extends TurbineData> {

    /**
     * @param name
     * @return {@link ClusterMonitor}<T>
     */
    public ClusterMonitor<T> getClusterMonitor(String name);

    /**
     * Init all the necessary cluster monitors
     */
    public void initClusterMonitors();

    /**
     * shutdown all the necessary cluster monitors
     */
    public void shutdownClusterMonitors();
}
