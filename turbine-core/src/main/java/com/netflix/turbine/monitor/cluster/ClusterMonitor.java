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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.turbine.data.AggDataFromCluster;
import com.netflix.turbine.data.DataFromSingleInstance;
import com.netflix.turbine.data.TurbineData;
import com.netflix.turbine.data.meta.MetaInfoUpdator;
import com.netflix.turbine.data.meta.MetaInformation;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.discovery.InstanceObservable;
import com.netflix.turbine.discovery.InstanceObservable.InstanceObserver;
import com.netflix.turbine.handler.TurbineDataDispatcher;
import com.netflix.turbine.handler.TurbineDataHandler;
import com.netflix.turbine.monitor.MonitorConsole;
import com.netflix.turbine.monitor.TurbineDataMonitor;
import com.netflix.turbine.monitor.instance.InstanceMonitor;
import com.netflix.turbine.monitor.instance.InstanceUrlClosure;

/**
 * A class that represents a {@link TurbineDataMonitor} for a cluster of {@link Instance}s. 
 * <p>The ClusterMonitor exhibits functionality that can be used for any extending class to manage it's respective {@link InstanceMonitor}s
 * as {@link Instance}s go up and down in a dynamic environment and ensure that they receive data from the most up to date set of hosts. 
 * 
 * <p>This includes
 * <ul>
 * <li>Managing all InstanceMonitors in the {@link MonitorConsole} to help keep InstanceMonitor start idempotent
 * <li>Starting new InstanceMonitors as new Instances appear - maybe due to a deployment or a new autoscale event etc. 
 * <li>Stopping the InstanceMonitors for Instances that are marked as down or have been terminated. 
 * <li>Notifying handlers below when it stops so that they can tear down appropriately. 
 * </ul>
 *
 * @param <K>
 */
public abstract class ClusterMonitor<K extends TurbineData> extends TurbineDataMonitor<K> {

    private static final Logger logger = LoggerFactory.getLogger(ClusterMonitor.class);
        
    protected final String name; 
    
    // stuff needed for cluster monitor management
    protected final TurbineDataDispatcher<K> clusterDispatcher;
    protected final MonitorConsole<K> clusterConsole;
    
    // stuff needed for host monitor management
    protected final TurbineDataDispatcher<DataFromSingleInstance> hostDispatcher;
    protected final MonitorConsole<DataFromSingleInstance> hostConsole;
    
    protected volatile boolean stopped = false;
    
    protected final Instance statsInstance; 
    protected final InstanceObservable instanceObservable;
    protected final InstanceUrlClosure urlClosure;
    protected final InstanceObserver monitorManager;
    
    private final AtomicInteger hostCount = new AtomicInteger(0);
    
    /**
     * @param name
     * @param clusterDispatcher : the dispatcher to dispatch cluster events to - e.g aggregated events
     * @param clusterConsole    : the console to register itself with, so that it can be discoverd by other listeners to cluster data
     * @param hostDispatcher    : the dispatcher to receive host events from
     * @param hostConsole       : the host console to maintain host connections in.
     * @param urlClosure        : the config dictating how to connect to a host.
     */
    public ClusterMonitor(String name, 
                               TurbineDataDispatcher<K> clusterDispatcher, MonitorConsole<K> clusterConsole,
                               TurbineDataDispatcher<DataFromSingleInstance> hostDispatcher, MonitorConsole<DataFromSingleInstance> hostConsole,
                               InstanceUrlClosure urlClosure) {
        this(name, 
             clusterDispatcher, clusterConsole,
             hostDispatcher, hostConsole,
             urlClosure,
             InstanceObservable.getInstance());
    }
    
    protected ClusterMonitor(String name, 
                                  TurbineDataDispatcher<K> cDispatcher, MonitorConsole<K> cConsole, 
                                  TurbineDataDispatcher<DataFromSingleInstance> hDispatcher, MonitorConsole<DataFromSingleInstance> hConsole,
                                  InstanceUrlClosure urlClosure,
                                  InstanceObservable instanceObservable) {
        this.name = name;
        this.clusterDispatcher = cDispatcher;
        this.clusterConsole = cConsole;
        
        this.hostDispatcher = hDispatcher;
        this.hostConsole = hConsole;
        
        this.urlClosure = urlClosure;
        this.instanceObservable = instanceObservable;
                
        this.monitorManager = new ClusterMonitorInstanceManager();
        
        this.statsInstance = new Instance(name, "clustetAgg", true);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Instance getStatsInstance() {
        return statsInstance;
    }

    /**
     * Start the monitor and register with the InstanceObservable to get updates on host status
     * @throws Exception
     */
    @Override
    public void startMonitor() throws Exception {
        // start up the monitor workers from here and register the event handlers 
        logger.info("Starting up the cluster monitor for " + name);
        instanceObservable.register(monitorManager);
        
        MetaInformation<K> metaInfo = getMetaInformation();
        if (metaInfo != null) {
            MetaInfoUpdator.addMetaInfo(metaInfo);
        }
    }

    /**
     * Stop the monitor, shut down resources that were created and notify listeners downstream about the event.
     */
    @Override
    public void stopMonitor() {
        logger.info("Stopping cluster monitor for " + name);
        stopped = true;
        // remove my event handler from all host monitors
        instanceObservable.deregister(monitorManager);
        // notify people below me that this monitor is shutting down
        clusterDispatcher.handleHostLost(getStatsInstance());
        // remove the handler from the host level dispatcher
        hostDispatcher.deregisterEventHandler(getEventHandler());
        // remove this monitor from the StatsEventConsole
        clusterConsole.removeMonitor(getName());
        
        clusterDispatcher.stopDispatcher();

        MetaInformation<K> metaInfo = getMetaInformation();
        if (metaInfo != null) {
            MetaInfoUpdator.removeMetaInfo(metaInfo);
        }

        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(new ObjectName("ClusterMonitorMBean:name=ClusterMonitorStats_" + name));
        } catch (Exception e) {
        }
    }
    
    /**
     * @return {@link MonitorConsole}<{@link DataFromSingleInstance}>
     */
    public MonitorConsole<DataFromSingleInstance> getInstanceMonitors() {
        return hostConsole;
    }

    @Monitor(name="hostCount", type=DataSourceType.GAUGE)
    public int getHostCount() {
        return hostCount.get();
    }
    
    public boolean isRunning() {
        return this.clusterDispatcher.running();
    }
    
    /**
     * @return {@link TurbineDataDispatcher}<K>
     */
    @Override
    public TurbineDataDispatcher<K> getDispatcher() {
        return clusterDispatcher;
    }

    /**
     * To be implemented by extending cluster monitors such as the aggregator.
     * 
     * @return {@link TurbineDataHandler}<{@link DataFromSingleInstance}>
     */
    public abstract TurbineDataHandler<DataFromSingleInstance> getEventHandler();
    
    /**
     * @return {@link ObservationCriteria}
     */
    public abstract ObservationCriteria getObservationCriteria();
    
    /**
     * Helper method that registers a listener to this cluster monitor. 
     * @param eventHandler
     */
    public void registerListenertoClusterMonitor(TurbineDataHandler<K> eventHandler) {
        
        TurbineDataHandler<K> oldHandler = getDispatcher().findHandlerForHost(getStatsInstance(), eventHandler.getName());
        if (oldHandler == null) {
            logger.info("Registering event handler for cluster monitor: " + eventHandler.getName());
            getDispatcher().registerEventHandler(getStatsInstance(), eventHandler);
            logger.info("All event handlers for cluster monitor: " +getDispatcher().getAllHandlerNames().toString());
        } else {
            logger.info("Handler: " + oldHandler.getName() + " already registered to host: " + getStatsInstance());
        }
    }

    /**
     * Track meta info for this cluster. This feature is optional. Do not override if you don't want meta info
     * from your cluster monitor.  
     * One can set the config property 'turbine.MetaInfoUpdator.enabled' to value 'false' to turn off the updator as well. 
     * 
     * @return
     */
    protected MetaInformation<K> getMetaInformation() {
        return null;
    }

    /**
     * Helper class that responds to hostup and hostdown events and thus can start / stop InstanceMonitors 
     *
     */
    public class ClusterMonitorInstanceManager implements InstanceObserver {

        @Override
        public String getName() {
            return name;
        }

        public void hostUp(Instance host) {

            if (!(getObservationCriteria().observeHost(host))) {
                return;
            }
            
            TurbineDataMonitor<DataFromSingleInstance> monitor = getMonitor(host);
            try {
                if(hostDispatcher.findHandlerForHost(host, getEventHandler().getName()) == null) {
                    // this handler is not already present for this host, then add it
                    hostDispatcher.registerEventHandler(host, getEventHandler());
                }
                monitor.startMonitor();
            } catch(Throwable t) {
                logger.info("Failed to start monitor: " + monitor.getName() + ", ex message: ", t);
                monitor.stopMonitor();
                logger.info("Removing monitor from stats event console");
                TurbineDataMonitor<DataFromSingleInstance> oldMonitor = hostConsole.removeMonitor(monitor.getName());
                if (oldMonitor != null) {
                    hostCount.decrementAndGet();
                }
            }
        }

        public void hostDown(Instance host) {
            TurbineDataMonitor<DataFromSingleInstance> hostMonitor = hostConsole.findMonitor(host.getHostname());
            if(hostMonitor != null) {
                hostCount.decrementAndGet();
                hostMonitor.stopMonitor();
                logger.info("Removing monitor from stats event console");
                hostConsole.removeMonitor(hostMonitor.getName());
            }
        }
        
        private TurbineDataMonitor<DataFromSingleInstance> getMonitor(Instance host) {
            
            TurbineDataMonitor<DataFromSingleInstance> monitor = hostConsole.findMonitor(host.getHostname());
            if (monitor == null) {
                monitor = new InstanceMonitor(host, urlClosure, hostDispatcher, hostConsole);
                hostCount.incrementAndGet();
                return hostConsole.findOrRegisterMonitor(monitor);
            } else {
                return monitor;
            }
        }

        @Override
        public void hostsUp(Collection<Instance> hosts) {
            for (Instance host: hosts) {
                try {
                    hostUp(host);
                } catch(Throwable t) {
                    logger.error("Could not start monitor on hostUp: " + host.toString(), t);
                }
            }
        }

        @Override
        public void hostsDown(Collection<Instance> hosts) {
            for (Instance host: hosts) {
                try {
                    hostDown(host);
                } catch(Throwable t) {
                    logger.error("Could not stop monitor on hostDown: " + host.toString(), t);
                }
            }
        }
    }
    

    @RunWith(MockitoJUnitRunner.class)
    public static class UnitTest {

        // the cluster related stuff
        @Mock private TurbineDataDispatcher<AggDataFromCluster> cDispatcher; 
        @Mock private MonitorConsole<AggDataFromCluster> cConsole; 
        
        // the host monitor related stuff
        @Mock private TurbineDataDispatcher<DataFromSingleInstance> hDispatcher;
        @Mock private MonitorConsole<DataFromSingleInstance> hConsole; 
        
        @Mock private InstanceObservable iObservable;
        protected InstanceUrlClosure testUrlClosure = new InstanceUrlClosure() {
            @Override
            public String getUrlPath(Instance host) {
                return "";
            }
        };
        
        @Mock private TurbineDataHandler<DataFromSingleInstance> handler;
        @Mock private ObservationCriteria mCriteria;
        
        @Test
        public void testCleanStartupAndShutdown() throws Exception {
           
            TestClusterMonitor monitor = new TestClusterMonitor();
            
            monitor.startMonitor();
     
            verify(iObservable).register(monitor.monitorManager);
            
            monitor.stopMonitor();
            
            verify(iObservable).deregister(monitor.monitorManager);
            verify(cDispatcher).handleHostLost(monitor.statsInstance);
            verify(cDispatcher).stopDispatcher();
            verify(hDispatcher).deregisterEventHandler(handler);
            verify(cConsole).removeMonitor(monitor.getName());
        }
        
        @Test
        public void testHostUp() throws Exception {
            
            InstanceMonitor hostMon = mock(InstanceMonitor.class); 
            when(hConsole.findMonitor(any(String.class))).thenReturn(hostMon);
            
            when(mCriteria.observeHost(any(Instance.class))).thenReturn(true);
            
            TestClusterMonitor monitor = new TestClusterMonitor();
            
            Instance host1 = new Instance("testHost1", "testCluster", true);
            
            monitor.monitorManager.hostsUp(Collections.singletonList(host1));
     
            verify(hConsole).findMonitor(host1.getHostname());
            verify(hostMon).startMonitor();
            verify(hDispatcher).registerEventHandler(host1, handler);
        }
        
        @Test
        public void testHostDown() throws Exception {
            
            Instance host1 = new Instance("testHost1", "testCluster", false);

            InstanceMonitor hostMon = mock(InstanceMonitor.class); 
            when(hConsole.findMonitor(any(String.class))).thenReturn(hostMon);
            when(hostMon.getName()).thenReturn(host1.getHostname());
            
            TestClusterMonitor monitor = new TestClusterMonitor();
            
            
            monitor.monitorManager.hostsDown(Collections.singletonList(host1));
     
            verify(hConsole).findMonitor(host1.getHostname());
            verify(hostMon).stopMonitor();
            verify(hConsole).removeMonitor("testHost1");
        }
        
        private class TestClusterMonitor extends ClusterMonitor<AggDataFromCluster> {

            public TestClusterMonitor() {
                super("testMonitor", cDispatcher, cConsole, hDispatcher, hConsole, testUrlClosure, iObservable);
            }

            @Override
            public TurbineDataHandler<DataFromSingleInstance> getEventHandler() {
                return handler;
            }

            @Override
            public ObservationCriteria getObservationCriteria() {
                return mCriteria;
            }
        }
    }
}