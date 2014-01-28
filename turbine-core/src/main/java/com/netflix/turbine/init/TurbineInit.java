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
package com.netflix.turbine.init;

import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConcurrentCompositeConfiguration;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicConfiguration;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.turbine.data.meta.MetaInfoUpdator;
import com.netflix.turbine.discovery.ConfigPropertyBasedDiscovery;
import com.netflix.turbine.discovery.FileBasedInstanceDiscovery;
import com.netflix.turbine.discovery.InstanceDiscovery;
import com.netflix.turbine.discovery.InstanceObservable;
import com.netflix.turbine.monitor.cluster.AggregateClusterMonitor;
import com.netflix.turbine.monitor.cluster.ClusterMonitorFactory;
import com.netflix.turbine.monitor.instance.InstanceMonitor;
import com.netflix.turbine.monitor.instance.StaleConnectionMonitorReaper;
import com.netflix.turbine.plugins.DefaultAggregatorFactory;
import com.netflix.turbine.plugins.PluginsFactory;

/**
 * Class that is used to start Turbine.
 * <p>Note that it needs an {@link InstanceDiscovery} to be registered for getting host information and {@link ClusterMonitorFactory}
 * as well to start the aggregation monitor.
 *
 * <p>By default the {@link FileBasedInstanceDiscovery} and the {@link AggregateClusterMonitor} are used.
 *
 */
public class TurbineInit {

    private static final Logger logger = LoggerFactory.getLogger(TurbineInit.class);

    private static final DynamicStringProperty InstanceDiscoveryClassProp = DynamicPropertyFactory.getInstance().getStringProperty("InstanceDiscovery.impl", null);

    @SuppressWarnings("rawtypes")
    public static void init() {

        ClusterMonitorFactory clusterMonitorFactory = PluginsFactory.getClusterMonitorFactory();
        if(clusterMonitorFactory == null) {
            PluginsFactory.setClusterMonitorFactory(new DefaultAggregatorFactory());
        }

        PluginsFactory.getClusterMonitorFactory().initClusterMonitors();

        InstanceDiscovery instanceDiscovery = PluginsFactory.getInstanceDiscovery();
        if (instanceDiscovery == null) {
            PluginsFactory.setInstanceDiscovery(getInstanceDiscoveryImpl());
        }

        InstanceObservable.getInstance().start(PluginsFactory.getInstanceDiscovery());
    }

    public static void stop() {

        PluginsFactory.getClusterMonitorFactory().shutdownClusterMonitors();

        InstanceMonitor.stop();

        InstanceObservable.getInstance().stop();

        StaleConnectionMonitorReaper.Instance.stop();

        if (ConfigurationManager.getConfigInstance() instanceof DynamicConfiguration) {
            ((DynamicConfiguration) ConfigurationManager.getConfigInstance()).stopLoading();
        }

        if (ConfigurationManager.getConfigInstance() instanceof ConcurrentCompositeConfiguration) {
            ConcurrentCompositeConfiguration config = ((ConcurrentCompositeConfiguration) ConfigurationManager.getConfigInstance());
            for(AbstractConfiguration innerConfig : config.getConfigurations()) {
                if (innerConfig instanceof DynamicConfiguration) {
                    ((DynamicConfiguration) innerConfig).stopLoading();
                }
            }
        }

        MetaInfoUpdator.Instance.stop();
    }

    private static InstanceDiscovery getInstanceDiscoveryImpl() {

        String className = InstanceDiscoveryClassProp.get();
        if (className == null) {
            logger.info("Property " + InstanceDiscoveryClassProp.getName() + " is not defined, hence using " + ConfigPropertyBasedDiscovery.class.getSimpleName() + " as InstanceDiscovery impl");
            return new ConfigPropertyBasedDiscovery();
        }

        try {
            Class clazz = Class.forName(className);
            return (InstanceDiscovery) clazz.newInstance();
        } catch (Exception e) {
            logger.error("Could not load InstanceDiscovery impl class", e);
            throw new RuntimeException(e);
        }
    }
}
