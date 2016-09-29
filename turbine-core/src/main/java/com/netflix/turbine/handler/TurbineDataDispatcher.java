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
package com.netflix.turbine.handler;

import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.turbine.data.DataFromSingleInstance;
import com.netflix.turbine.data.TurbineData;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.monitor.TurbineDataMonitor;
import com.netflix.turbine.monitor.cluster.ClusterMonitor;
import com.netflix.turbine.monitor.instance.InstanceMonitor;
import com.netflix.turbine.monitor.MonitorConsole;

/**
 * Class the manages the mapping from {@link InstanceMonitor} connections to {@link TurbineDataHandler} listeners.
 * 
 * <p>When registering a {@link TurbineDataHandler} with the dispatcher, one must specify the {@link Instance} or host that the handler is interested in receiving data from. 
 * <p>The dispatcher maintains a {@link HandlerQueueTuple} object for each registered handler and hence all handlers can receive data from multiple {@link InstanceMonitor} objects. 
 * <p>This is how the {@link ClusterMonitor} object actually works. For each {@link Instance} that it needs to monitor, it opens a connection to that host using 
 * an {@link InstanceMonitor} and then stores that object in a {@link MonitorConsole} for later use and discovery. The ClusterMonitor then also registers it's {{@link TurbineDataHandler}
 * as a listener to that host in the dispatcher so that it can receive events. This way a single handler can be registered for multiple host monitors.
 * 
 * <p>Note that another use of the dispatcher is  the opposite case where multiple listeners can express interest in data from the same host. In this case
 * we can have multiple {@link TurbineDataHandler} objects registered to the same {@link InstanceMonitor}  via the dispatcher. 
 *
 * @see InstanceMonitor
 * @param <K>
 */
public class TurbineDataDispatcher<K extends TurbineData> {

    private static final Logger logger = LoggerFactory.getLogger(TurbineDataDispatcher.class);
    
    // list of all event handlers (by host) that are interested in getting the data
    private final ConcurrentHashMap<Instance, ConcurrentHashMap<String, HandlerQueueTuple<K>>> eventHandlersForHosts;

    // keeping track of event listeners for each host, so that we can notify the publisher when there are no more
    // so that he may decide to stop publishing
    private ConcurrentHashMap<Instance, AtomicInteger> iterationsWithoutHandlers;
    
    // keep track of handler tuples for each handler. 
    private ConcurrentHashMap<String, HandlerQueueTuple<K>> handlerTuples; 
    
    // boolean indicating whether the dispatcher has been stopped or not. 
    private volatile boolean stopped = false; 
        
    // the name for this dispatcher
    private final String name; 
    
    /**
     * Public constructor
     */
    public TurbineDataDispatcher(String name) {
        
        this.name = name; 
        
        eventHandlersForHosts = new ConcurrentHashMap<Instance, ConcurrentHashMap<String, HandlerQueueTuple<K>>>();
        iterationsWithoutHandlers = new ConcurrentHashMap<Instance, AtomicInteger>();
        handlerTuples = new ConcurrentHashMap<String, HandlerQueueTuple<K>>();
    }
    
    public String getName() {
        return name;
    }

    /**
     * This is meant to be called when a monitor is shutting down. 
     * @param host
     */
    public void handleHostLost(Instance host) {
       
        Map<String, HandlerQueueTuple<K>> eventHandlers = eventHandlersForHosts.remove(host);

        if(eventHandlers != null) {
            for(HandlerQueueTuple<K> tuple : eventHandlers.values()) {
                tuple.getHandler().handleHostLost(host);
            }
            eventHandlers.clear();
        }
        iterationsWithoutHandlers.remove(host);
    }

    /**
     * Register event listener to this host
     * @param host
     * @param handler
     */
    public void registerEventHandler(Instance host, TurbineDataHandler<K> handler) {
        
        ConcurrentHashMap<String, HandlerQueueTuple<K>> eventHandlers = eventHandlersForHosts.get(host);
        if(eventHandlers == null) {
            // this is the first handler for this host. 
            eventHandlers = new ConcurrentHashMap<String, HandlerQueueTuple<K>>();
            ConcurrentHashMap<String, HandlerQueueTuple<K>> prevHandlers = eventHandlersForHosts.putIfAbsent(host, eventHandlers);
            if(prevHandlers != null) {
                // another thread beat us to this. Use the object that it created instead
                eventHandlers = prevHandlers;
            }
        }
        
        HandlerQueueTuple<K> previousTuple = eventHandlers.get(handler.getName());
        if (previousTuple != null) {
            throw new RuntimeException("Handler has already been registered: " + handler.getName() + ", existing handlers: " + eventHandlers.keySet());
        }
        
        HandlerQueueTuple<K> tuple = getHandlerQueueTuple(handler);
        if (!tuple.previouslyStopped()) {
            previousTuple = eventHandlers.putIfAbsent(handler.getName(), tuple);
            if(previousTuple != null) {
                // This is IMP. We shouldn't be registering the same handler when another one exists. This means that the calling code is not tracking the event handlers effectively.
                throw new RuntimeException("Handler has already been registered: " + handler.getName() + ", existing handlers: " + eventHandlers.keySet());
            }
            //logger.info("Regisering : " + handler.getName()  + " to host : " + host.getHostname() + " isRunning: " + tuple.running());
        } else {
            logger.info("Found handler tuple to be stopped: " + handler.getName()  + " will not associate with host : " + host.getHostname());
        }
    }

    /**
     * Private helper to instantiate a handler tuple in a thread safe manner
     * @param handler
     * @param queueSize
     * @param numThreads
     * @return
     */
    private HandlerQueueTuple<K> getHandlerQueueTuple(TurbineDataHandler<K> handler) {
        HandlerQueueTuple<K> previousTuple = handlerTuples.get(handler.getName());
        if(previousTuple != null) {
            return previousTuple;
        }
        HandlerQueueTuple<K> tuple = new HandlerQueueTuple<K>(handler);
        previousTuple = handlerTuples.putIfAbsent(handler.getName(), tuple);
        if(previousTuple != null) {
            return previousTuple;
        }
        // start the dispatchers for each of the handlers
        logger.info("\n\nJust added and starting handler tuple: " + handler.getName());
        try {
            tuple.start();
            return tuple;
        } catch (Exception e) {
            logger.error("Caught failure when registering handler");
            this.deregisterEventHandler(handler);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * De register an event listener
     * @param handlerName
     */
    public void deregisterEventHandler(String handlerName) {
        
        logger.info("Removing event handler: " + handlerName);   
        // Note that we need to do this for each of the hosts, coz some event handlers (like cluster monitors) would be subscribed for all hosts 
        
        HandlerQueueTuple<K> tuple = handlerTuples.get(handlerName);
        if (tuple != null) {
            tuple.stop();
            HandlerQueueTuple<K> key = handlerTuples.remove(handlerName);
            logger.info(" tuples : " + handlerTuples.keySet());
            logger.info("Removed handler queue tuple for handler: " + key);
        }
            
        // remove the handler from each of the hosts
        for(Instance host : eventHandlersForHosts.keySet()) {
            ConcurrentHashMap<String, HandlerQueueTuple<K>> eventHandlers = eventHandlersForHosts.get(host);
            HandlerQueueTuple<K> handlerTupleForThisHost = eventHandlers.remove(handlerName);
            if(handlerTupleForThisHost != null) {
                handlerTupleForThisHost.getHandler().handleHostLost(host);
            }
        }
    }

    /**
     * Deregister an event listener
     * @param handler
     */
    public void deregisterEventHandler(TurbineDataHandler<K> handler) {
        deregisterEventHandler(handler.getName());
    }
    
    /**
     * Helper method to check if a handler is registered for a host. This can help callers effectively manage their event handlers
     * @param host
     * @param handlerName
     * @return StatsEventHandler<K>
     */
    public TurbineDataHandler<K> findHandlerForHost(Instance host, String handlerName) {
        
        Map<String, HandlerQueueTuple<K>> eventHandlers = eventHandlersForHosts.get(host);
        if(eventHandlers != null) {
            HandlerQueueTuple<K> tuple = eventHandlers.get(handlerName);
            if(tuple != null) {
                return tuple.getHandler();
            }
        }
        return null;
    }
    
    /**
     * Common loop life-cycle logic.
     * <p>
     * This will asynchronously invoke handleData on all listeners. 
     * We do it asynchronously because we can potentially have quite a few handlers and some of them with delays which we don't want to have impact all other handlers.
     * 
     * @param host
     * @param statsData
     * @return boolean: true/false indicating whether there are any listeners left 
     * @throws InterruptedException
     */
    public boolean pushData(final Instance host, final Collection<K> statsData) {

        if(stopped) {
            return false;
        }
        
        // get a copy of the list so we don't have ConcurrentModification errors when it changes while we're iterating
        Map<String, HandlerQueueTuple<K>> eventHandlers = eventHandlersForHosts.get(host);
        if (eventHandlers == null) {
            return false;  // stop the monitor, this should generally not happen, since we generally manage a set of static listeners for all hosts in discovery
        }

        for (final HandlerQueueTuple<K> tuple : eventHandlers.values()) {
            tuple.pushData(statsData);
        }
        
        // keep track of listeners registered, and if there are none, then notify the publisher of the events
        AtomicInteger count = getIterationWithoutHandlerCount(host);
        if (eventHandlers.size() == 0) {
            count.incrementAndGet();
            if (count.get() > 5) {
                logger.info("We no longer have handlers to dispatch to");
                return false;
            }
        } else {
            count.set(0);
        }
        return true;
    }
    

    public boolean pushData(final Instance host, final K statsData) {
        if(stopped) {
            return false; // short circuit, avoid creating the singleton list
        }
        
        // get a copy of the list so we don't have ConcurrentModification errors when it changes while we're iterating
        Map<String, HandlerQueueTuple<K>> eventHandlers = eventHandlersForHosts.get(host);
        if (eventHandlers == null) {
            return false;  // stop the monitor, this should generally not happen, since we generally manage a set of static listeners for all hosts in discovery
        }

        for (final HandlerQueueTuple<K> tuple : eventHandlers.values()) {
            tuple.pushData(statsData);
        }
        
        // keep track of listeners registered, and if there are none, then notify the publisher of the events
        AtomicInteger count = getIterationWithoutHandlerCount(host);
        if (eventHandlers.size() == 0) {
            count.incrementAndGet();
            if (count.get() > 5) {
                logger.info("We no longer have handlers to dispatch to");
                return false;
            }
        } else {
            count.set(0);
        }
        return true;
    }
    
    /**
     * Method that pushes data to a collection of handlers directly
     * @param handlers
     * @param statsData
     */
    public void pushData(final Collection<String> handlers, final Collection<K> statsData) {

        if(stopped) {
            return;
        }
        
        for (String handlerName : handlers) {
            HandlerQueueTuple<K> tuple = handlerTuples.get(handlerName);
            if (tuple != null) {
                tuple.pushData(statsData);
            }
        }
        return;
    }
    
    public void pushData(final Collection<String> handlers, final K statsData) {

        if(stopped) {
            logger.info("Dispatcher has been stopped, will not deliver data");
            return;
        }
        
        for (String handlerName : handlers) {
            HandlerQueueTuple<K> tuple = handlerTuples.get(handlerName);
            if (tuple != null) {
                tuple.pushData(statsData);
            }
        }
        return;
    }
    
    /**
     * Stop the dispatcher. Shutdown everything
     * This notifies everyone downstream that all hosts are lost (a hacky mechanism to cause all event handlers to eject)
     * It also sets the stopped status which will notify monitors as they try to send data 
     */
    public void stopDispatcher() {
        
        stopped = true; 
        
        for (HandlerQueueTuple<K> tuple : handlerTuples.values()) {
            tuple.stop();
            HandlerQueueTuple<K> key = handlerTuples.remove(tuple.getHandler().getName());
            logger.info(" tuples : " + handlerTuples.keySet());
            logger.info("Key: " + key);
        }
        handlerTuples.clear();

        for(Instance host : eventHandlersForHosts.keySet()) {
            handleHostLost(host);
            Map<String, HandlerQueueTuple<K>> eventHandlers = eventHandlersForHosts.get(host);
            if (eventHandlers != null) {
                eventHandlers.clear();
            }
        }
        eventHandlersForHosts.clear();
    }
    
    /**
     * Helper to init/retrieve the count for each host, where there have been no listeners. 
     * This helps the monitors decide to shut down
     * @param host
     * @return
     */
    private AtomicInteger getIterationWithoutHandlerCount(Instance host) {
        
        AtomicInteger count = iterationsWithoutHandlers.get(host);
        if(count == null) {
            count = new AtomicInteger(0);
            AtomicInteger prevCount = iterationsWithoutHandlers.putIfAbsent(host, count);
            if(prevCount != null) {
                count = prevCount;
            }
        }
        return count;
    }
    
    public boolean running() {
        for (HandlerQueueTuple<K> tuple : handlerTuples.values()) {
            if(tuple.running()) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Helpful method used to print out all the handler names. Used for jmx stats
     * @return Set<String>
     */
    public Set<String> getAllHandlerNames() {
        return handlerTuples.keySet();
    }

    @SuppressWarnings("unchecked")
    public static class UnitTest {

        Instance instance = new Instance("test", "cluster", true);
        
        public class TestData extends TurbineData {

            public TestData(String type, String name) {
                super(null, type, name);
            }

            @Override
            public HashMap<String, Long> getNumericAttributes() {
                return null;
            }

            @Override
            public HashMap<String, String> getStringAttributes() {
                return null;
            }

            @Override
            public HashMap<String, Map<String, ? extends Number>> getNestedMapAttributes() {
                return null;
            }
        }
        
        private static PerformanceCriteria perfCriteria = new PerformanceCriteria() {
            @Override
            public boolean isCritical() {
                return false;
            }
            @Override
            public int getMaxQueueSize() {
                return 1;
            }
            @Override
            public int numThreads() {
                return 1;
            }
        };
        
        @Test (expected=RuntimeException.class)
        public void testRegisterDuplicateHandler() throws Exception {
            
            TurbineDataDispatcher<TestData> dispatcher = new TurbineDataDispatcher<TestData>("TEST");
            TurbineDataHandler<TestData> h1 = mock(TurbineDataHandler.class);
            when(h1.getName()).thenReturn("h1");
            when(h1.getCriteria()).thenReturn(perfCriteria);
            dispatcher.registerEventHandler(instance, h1);
            dispatcher.registerEventHandler(instance, h1);
        }
        
        @Test
        public void testRegsiterAndDeregister() throws Exception {
            
            Collection<TestData> dataSet = Collections.singletonList(new TestData(null, null));

            TurbineDataMonitor<DataFromSingleInstance> monitor = mock(TurbineDataMonitor.class);
            when(monitor.getName()).thenReturn("publisher");
            
            TurbineDataDispatcher<TestData> dispatcher = new TurbineDataDispatcher<TestData>("TEST");
            
            TurbineDataHandler<TestData> h1 = mock(TurbineDataHandler.class);
            TurbineDataHandler<TestData> h2 = mock(TurbineDataHandler.class);
            when(h1.getName()).thenReturn("h1");
            when(h2.getName()).thenReturn("h2");
            when(h1.getCriteria()).thenReturn(perfCriteria);
            when(h2.getCriteria()).thenReturn(perfCriteria);
            
            dispatcher.registerEventHandler(instance, h1);
            dispatcher.registerEventHandler(instance, h2);
            dispatcher.pushData(instance, dataSet);

            Thread.sleep(1000);
            
            verify(h1, times(1)).handleData(anyList());

            dispatcher.deregisterEventHandler(h1);
            dispatcher.pushData(instance, dataSet);
            
            verify(h1, times(1)).handleData(anyList());
            
            dispatcher.deregisterEventHandler(h1);
            dispatcher.deregisterEventHandler(h2);
            for(int i=0; i<5; i++) {
                assertTrue(dispatcher.pushData(instance, dataSet));
            }

            verify(h1, times(1)).handleData(anyList());

            for(int i=0; i<4; i++) {
                assertFalse(dispatcher.pushData(instance, dataSet));
            }
            
            dispatcher.stopDispatcher();
        }
        
        @Test
        public void testEventRejection() throws Exception {
            
            TestData data = new TestData(null, null);
            final Collection<TestData> dataSet = Collections.singletonList(data);

            TurbineDataMonitor<TestData> monitor = mock(TurbineDataMonitor.class);
            when(monitor.getName()).thenReturn("publisher");
            
            final TurbineDataDispatcher<TestData> dispatcher = new TurbineDataDispatcher<TestData>("TEST");

            TurbineDataHandler<TestData> h1 = mock(TurbineDataHandler.class);
            TurbineDataHandler<TestData> h2 = mock(TurbineDataHandler.class);
            when(h1.getName()).thenReturn("h1");
            when(h2.getName()).thenReturn("h2");
            when(h1.getCriteria()).thenReturn(perfCriteria);
            when(h2.getCriteria()).thenReturn(perfCriteria);
            
            dispatcher.registerEventHandler(instance, h1);
            dispatcher.registerEventHandler(instance, h2);
            
            Answer<Void> answer = new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    Thread.sleep(10);  // cause the event handlers to be slow, this will cause events to get rejected
                    return null;
                }
            };
                
            doAnswer(answer).when(h1).handleData(dataSet);
            
            final AtomicLong totalCounter = new AtomicLong(0);
            
            ExecutorService executor = Executors.newFixedThreadPool(100);
            for(int i=0; i<100; i++) {
                executor.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        while(true) {
                            dispatcher.pushData(instance, dataSet);
                            totalCounter.incrementAndGet();
                            Thread.sleep(10);
                        }
                    }
                });
            }
            
            Thread.sleep(3000);
            
            executor.shutdownNow();

            dispatcher.stopDispatcher();
        }
    }
}