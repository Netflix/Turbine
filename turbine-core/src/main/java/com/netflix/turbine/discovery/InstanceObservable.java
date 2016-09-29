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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.turbine.plugins.PluginsFactory;

/**
 * Class the represents a continuous poller that fetches {@link Instance} information from the specified {@link InstanceDiscovery}
 * object. The InstanceDiscovery must be registered with the {@link PluginsFactory} at startup, else Turbine uses a default
 * {@link FileBasedInstanceDiscovery} mechanism. 
 * 
 * Note that the InstanceObservable runs in continuous loop at a periodic interval specified by the config property <b>turbine.discovery.pollDelayMillis</b>
 * with a default of <b>60 seconds</b>
 * 
 * <p>It checks the {@link Instance#isUp()} state to understand whether to notify observers downstream about hosts that are up and down. 
 * <p>It also keeps state from the previous poll and it uses this to determine whether some older hosts 
 * have disappeared.
 * 
 * <p>One can register a {@link InstanceObserver} by using the {@link #register(InstanceObserver)} to receive periodic updates of hosts that are up
 * and down via the {@link InstanceObserver#hostsUp(Collection)} and the {@link InstanceObserver#hostsDown(Collection)} callbacks.

 */
public class InstanceObservable {

    private static final Logger logger = LoggerFactory.getLogger(InstanceObservable.class);

    /**
     * Interface for representing an observer to the {@link InstanceObservable} 
     */
    public interface InstanceObserver {
        
        /**
         * The unique name of this observer. 
         * @return String
         */
        public String getName(); 

        /**
         * Callback for notifying hosts that are up. 
         * @param hosts Collection<{@link Instance}> hosts
         */
        public void hostsUp(Collection<Instance> hosts);

        /**
         * Callback for notifying hosts that are down. 
         * @param hosts Collection<{@link Instance}> hosts
         */
        public void hostsDown(Collection<Instance> hosts);
    }

    // the singleton instance
    private static final InstanceObservable INSTANCE = new InstanceObservable();

    /**
     * @return {@link InstanceObservable}
     */
    public static InstanceObservable getInstance() {
        return INSTANCE;
    }

    // how often to run
    private final DynamicIntProperty pollDelayMillis = DynamicPropertyFactory.getInstance().getIntProperty("turbine.discovery.pollDelayMillis", 60000);
    // thread safe reference to the current state. Also used to diff with next state to understand if any host dropped out. 
    private final AtomicReference<CurrentState> currentState = new AtomicReference<CurrentState>(new CurrentState()); 
    // thread safe map of observers. 
    private final ConcurrentHashMap<String, InstanceObserver> observers = new ConcurrentHashMap<String, InstanceObserver>();

    // keep track of no of hosts up in discovery 
    private final AtomicReference<Map<String, Integer>> hostsUpPerCluster = new AtomicReference<Map<String, Integer>>(new HashMap<String, Integer>()); 

    private final Timer timer = new Timer();

    // flag indicating if the observable has already started, this makes start idempotent
    private final AtomicBoolean started = new AtomicBoolean(false);
    // metrics purposes only
    private final AtomicInteger heartbeat = new AtomicInteger();

    // the underlying source of info
    private InstanceDiscovery instanceDiscovery; 

    /**
     * Private no-arg constructor
     */
    private InstanceObservable() {
    }

    @Monitor(name = "HostUp", type = DataSourceType.GAUGE)
    public int getCurrentHostUpCount() {
        return getCurrentHostsUp().size();
    }

    /**
     * @return HashSet<{@link Instance}>
     */
    public HashSet<Instance> getCurrentHostsUp() {
        return currentState.get().hostsUp;
    }

    @Monitor(name = "HostDown", type = DataSourceType.GAUGE)
    public int getCurrentHostDownCount() {
        return getCurrentHostsDown().size();
    }

    @Monitor(name = "Heartbeat", type = DataSourceType.COUNTER)
    public int getHeartbeat() {
        return heartbeat.get();
    }

    /**
     * @return HashSet<{@link Instance}>
     */
    public HashSet<Instance> getCurrentHostsDown() {
        return currentState.get().hostsDown;
    }

    /**
     * @return Set<String>
     */
    public Set<String> getObservers() {
        return observers.keySet();
    }

    /**
     * Idempotent call to start the Observable. Caller must provide the {@link InstanceDiscovery} implementation
     * @param iDiscovery
     */
    public void start(InstanceDiscovery iDiscovery) {
        if (started.get()) {
            throw new RuntimeException("InstanceDiscovery already started");
        }
        if (iDiscovery == null) {
            throw new RuntimeException("InstanceDiscovery is null");
        }
        instanceDiscovery = iDiscovery;
        logger.info("Starting InstanceObservable at frequency: " + pollDelayMillis.get() + " millis");
        timer.schedule(producer, 0, pollDelayMillis.get());
        started.set(true);
    }

    /**
     * Stop the {@link InstanceObservable}
     */
    public void stop() {
        logger.info("InstanceObservable shutting down");
        timer.cancel();
        observers.clear();
    }

    /**
     * Register with the observable to get callbacks on fleet status updates
     * @param watcher {@link InstanceObserver}
     * @return {@link InstanceObserver} if it already exists
     */
    public InstanceObserver register(InstanceObserver watcher) {

        InstanceObserver previous = observers.putIfAbsent(watcher.getName(), watcher);
        if(previous != null) {
            return previous;
        } else {
            return watcher;
        }
    }

    /**
     * Deregister from the observable
     * @param watcher {@link InstanceObserver}
     */
    public void deregister(InstanceObserver watcher) {
        observers.remove(watcher.getName());
    }
    
    /**
     * Helper method to give us the hosts up for a specified cluster
     * @param cluster
     * @return int
     */
    public int getNumHostsUpForCluster(String cluster) {
        Integer hostCount = hostsUpPerCluster.get().get(cluster);
        if (hostCount == null) {
            return 0;
        } else {
            return hostCount.intValue();
        }
    }

    /**
     * Class representing the current state of the Instance information across the system.
     */
    private class CurrentState {

        private final HashSet<Instance> hostsUp;
        private final HashSet<Instance> hostsDown;

        private CurrentState() {
            hostsUp = new HashSet<Instance>();
            hostsDown = new HashSet<Instance>();
        }
    }

    /**
     * TimerTask that runs at a periodic rate.
     */
    private final TimerTask producer = new TimerTask() {

        @Override
        public void run() {
            if(observers == null || observers.size() == 0) {
                logger.info("No observers for InstanceObservable, will try again later");
                return; 
            }

            // get the refreshed cluster
            List<Instance> newList = null;
            try {
                newList = getInstanceList();

                logger.info("Retrieved hosts from InstanceDiscovery: " + newList.size());
                if(newList.size() < 10) {
                    logger.debug("Retrieved hosts from InstanceDiscovery: " + newList);
                }

                // The total set of hosts in the previous round (both sets up and down combined)
                List<Instance> previousList = new ArrayList<Instance>(currentState.get().hostsUp);

                // Now subtract out the total set of new hosts found from the previous list. 
                // This should give us any hosts that disappeared quickly between this round and the previous round  
                previousList.removeAll(newList);

                logger.info("Found hosts that have been previously terminated: " + previousList.size());

                // clear the previous state
                CurrentState newState = new CurrentState();

                // set the current state
                for(Instance host: newList) {
                    if(host.isUp()) {
                        newState.hostsUp.add(host);
                    } else {
                        newState.hostsDown.add(host);
                    }
                }

                // add hosts that disappeared between this round and the previous round only. 
                newState.hostsDown.addAll(previousList);

                logger.info("Hosts up:" + newState.hostsUp.size() + ", hosts down: " + newState.hostsDown.size());

                // setting the atomic reference
                currentState.set(newState);

                // Notify watchers of fleet changes
                for(InstanceObserver watcher: observers.values()) {
                    if(currentState.get().hostsUp.size() > 0) {
                        try {
                            watcher.hostsUp(currentState.get().hostsUp);
                        } catch(Throwable t) {
                            logger.error("Could not call hostUp on watcher: " + watcher.getName(), t);
                        }
                    }
                    if (currentState.get().hostsDown.size() > 0) {
                        try {
                            watcher.hostsDown(currentState.get().hostsDown);
                        } catch(Throwable t) {
                            logger.error("Could not call hostDown on watcher: " + watcher.getName(), t);
                        }
                    }
                }

                updateHostsCountsPerCluster(currentState.get().hostsUp);

                heartbeat.incrementAndGet();

            } catch (Throwable t) {
                logger.info("Failed to fetch instance info, will continue to run and will try again later", t);
                return;
            }
            return;
        }
    };

    private List<Instance> getInstanceList() throws Exception {
        List<Instance> instances = new ArrayList<Instance>();
        if(instanceDiscovery != null) {
            instances.addAll(instanceDiscovery.getInstanceList());
        }
        return instances;
    }

    private void updateHostsCountsPerCluster(HashSet<Instance> hostsUp) {

        if (hostsUp == null || hostsUp.size() == 0) {
            return;
        }

        Map<String, Integer> map = new HashMap<String, Integer>();

        for(Instance host : hostsUp) {

            String cluster = host.getCluster();
            if (cluster == null) {
                continue;
            }
            Integer hostCount =  map.get(cluster);
            if (hostCount == null) {
                hostCount = new Integer(0);
            }
            hostCount++;
            map.put(cluster, hostCount);
        }
        if (map.size() > 0) {
            hostsUpPerCluster.set(map);
        }
    }

    public static class UnitTest {
                
        private InstanceDiscovery instanceDiscovery; 
                
        private LinkedBlockingQueue<List<Instance>> queue = new LinkedBlockingQueue<List<Instance>>();  

        @Test
        public void testObservableThreadCorrectlyReportsHostsUpAndDown() throws Exception {

            ConfigurationManager.getConfigInstance().setProperty("turbine.discovery.pollDelayMillis", 10);

            instanceDiscovery = mock(InstanceDiscovery.class);
            doAnswer(new Answer<List<Instance>>() {

                @Override
                public List<Instance> answer(InvocationOnMock invocation) throws Throwable {
                    System.out.println("waiting ...");
                    List<Instance> list = queue.take();
                    System.out.println("unblocked ...");
                    return list;
                }
            }).when(instanceDiscovery).getInstanceList();

            final Set<String> hostsUp = new HashSet<String>(); 
            final Set<String> hostsDown = new HashSet<String>(); 

            InstanceObserver observer = new InstanceObserver() {

                @Override
                public String getName() {
                    return "TestObserver";
                }

                @Override
                public void hostsUp(Collection<Instance> hosts) {
                    hostsUp.clear();
                    for(Instance host : hosts) {
                        System.out.println("Up: " + host.getHostname());
                        hostsUp.add(host.getHostname());
                    }
                }

                @Override
                public void hostsDown(Collection<Instance> hosts) {
                    hostsDown.clear();
                    for(Instance host : hosts) {
                        System.out.println("Down: " + host.getHostname());
                        hostsDown.add(host.getHostname());
                    }
                }
            };

            InstanceObservable observable = new InstanceObservable();
            observable.register(observer);

            observable.start(instanceDiscovery);

            List<Instance> list = new ArrayList<Instance>();
            list.add(new Instance("a", "cluster", true));
            list.add(new Instance("b", "cluster", false));
            list.add(new Instance("c", "cluster", true));

            queue.put(list);
            Thread.sleep(20);

            verifySet(hostsUp, "a", "c");
            verifySet(hostsDown, "b");

            list = new ArrayList<Instance>();
            list.add(new Instance("a", "cluster", false));

            queue.put(list);
            Thread.sleep(20);

            // Note that b should be forgotten here
            verifySet(hostsDown, "a", "c");

            list = new ArrayList<Instance>();
            list.add(new Instance("a", "cluster", false));
            list.add(new Instance("b", "cluster", true));
            list.add(new Instance("c", "cluster", true));

            queue.put(list);
            Thread.sleep(20);

            verifySet(hostsUp, "b", "c");
            verifySet(hostsDown, "a");

            list = new ArrayList<Instance>();
            list.add(new Instance("a", "cluster", true));
            list.add(new Instance("b", "cluster", true));
            list.add(new Instance("c", "cluster", false));

            queue.put(list);
            Thread.sleep(20);

            verifySet(hostsUp, "a", "b");
            verifySet(hostsDown, "c");

            list = new ArrayList<Instance>();
            list.add(new Instance("a", "cluster", true));
            list.add(new Instance("c", "cluster", false));
            list.add(new Instance("d", "cluster", true));

            queue.put(list);
            Thread.sleep(20);

            verifySet(hostsUp, "a", "d");
            verifySet(hostsDown, "b", "c");

            list = new ArrayList<Instance>();
            list.add(new Instance("a", "cluster", true));
            list.add(new Instance("c", "cluster", false));
            list.add(new Instance("d", "cluster", false));
            list.add(new Instance("e", "cluster", true));

            queue.put(list);
            Thread.sleep(20);

            verifySet(hostsUp, "a", "e");
            // note that we should not see 'b' as host down here, since we should eventually forget that state
            verifySet(hostsDown, "c", "d");  

            observable.stop();
        }

        private void verifySet(Set<String> set, String ... args) {

            for(String s : args) {
                assertTrue(set.remove(s));
            }
            assertTrue(set.isEmpty());
        }
    }
}

