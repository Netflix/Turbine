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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.turbine.data.AggDataFromCluster;
import com.netflix.turbine.data.ConcurrentEventQueue;
import com.netflix.turbine.data.DataFromSingleInstance;
import com.netflix.turbine.data.EventQueue;
import com.netflix.turbine.data.StatsRollingNumber;
import com.netflix.turbine.data.StatsRollingNumber.Type;
import com.netflix.turbine.data.TurbineData;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.monitor.cluster.AggregateClusterMonitor;
import com.netflix.turbine.monitor.instance.InstanceMonitor;
import com.netflix.turbine.utils.WorkerThread;
import com.netflix.turbine.utils.WorkerThread.Worker;

/**
 * Class that encapsulates that infrastructure needed by a {@link TurbineDataHandler} to receive data from multiple producers. 
 * 
 * <p>The {@link HandlerQueueTuple} object uses an {@link EventQueue} implementation to protect the publishers 
 * from a slow listener. It also employs a number of threads needed to poll repeatedly from the {@link EventQueue}
 * and dispatch to the handler underneath. If the EventQueue is full then {@link HandlerQueueTuple#pushData(Collection)} simply fails fast and moves on. 
 * 
 * <p>Note that the max <b>queue size</b> and the number of <b>threads</b> will help determine the throughput of a handler, and hence 
 * must be carefully chosen. 
 * <ul>
 * <li>When data is flowing at a roughly constant velocity then one can tune with a smaller queue assuming that there
 * are sufficient no of threads. 
 * <li>But when data is sparse and bursty, then we must select a larger queue size even if we use fewer threads. 
 * </ul>
 * 
 * <b>Use Cases</b>
 * <p>
 * The {@link AggregateClusterMonitor} employs this object to receive and aggregate data from multiple {@link InstanceMonitor} objects.
 * The aggregator can in turn also dispatch data to {@link TurbineDataHandler} underneath using a HandlerQueueTuple for each handler as well,
 * thus decoupling the performance of the aggregator from any potentially slow listener. 
 * 
 * @param <K>
 */
public class HandlerQueueTuple<K extends TurbineData> {

    private static final Logger logger = LoggerFactory.getLogger(AggDataFromCluster.class);
    
    // the underlying event handler
    private final TurbineDataHandler<K> eventHandler;
    // event queue
    private final EventQueue<K> queue;
    // worker threads
    private final List<WorkerThread> workerThreads;
    private final int numThreads; 
    private volatile boolean stopped = false;
    
    // used for stats only
    private StatsRollingNumber counter = new StatsRollingNumber(10000, 10);
    
    /**
     * @param eventHandler
     */
    public HandlerQueueTuple(TurbineDataHandler<K> eventHandler) {
        
        this.eventHandler = eventHandler;

        this.queue = new ConcurrentEventQueue<K>(eventHandler.getCriteria().getMaxQueueSize());
        
        this.numThreads = eventHandler.getCriteria().numThreads();
        this.workerThreads = new ArrayList<WorkerThread>(numThreads);
        this.stopped = false;
        
    }
    
    /**
     * Starts the poller threads and this causes the eventhandler to start receiving data via the {@link TurbineDataHandler#handleData(Collection)} method.
     * @throws Exception
     */
    public void start() throws Exception {
        if(stopped) {
            logger.info("\n\nTuple already stopped, will not start again, need to create new tuple");
            return;
        }
        for(int i=0 ; i<numThreads; i++) {
            PerHandlerDispatcher dispatcherForHandler = new PerHandlerDispatcher(queue, eventHandler, numThreads);
            WorkerThread thread = new WorkerThread(dispatcherForHandler, -1, false);
            workerThreads.add(thread);
            thread.start();
        }
    }
    
    /**
     * Stops all the poller threads, and the event handler will stop receiving data.
     */
    public void stop() {
        if(stopped) {
            return;
        }
        stopped = true;
        for (WorkerThread thread : workerThreads) {
            thread.stopAndBlock();
        }
        workerThreads.clear();
        logger.info("\n\nRemoving tuple for : " + eventHandler.getName() + " tuple running: " + running());
    }
    
    /**
     * Helper method to identify if the tuple has been requested to stop
     * @return true/false
     */
    public boolean previouslyStopped() {
        return stopped;
    }
    
    /**
     * Helper to identify if the tuple is still running
     * @return true/false
     */
    public boolean running() {
        for (WorkerThread thread : workerThreads) {
            if(thread.isRunning()) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isCritical() {
        return eventHandler.getCriteria().isCritical();
    }
    
    /**
     * @return {@link TurbineDataHandler}<K>
     */
    public TurbineDataHandler<K> getHandler() {
        return eventHandler;
    }

    /**
     * @return {@link EventQueue}<K> 
     */
    public EventQueue<K> getQueue() {
        return queue;
    }
    
    /**
     * Send data to the tuple
     * @param statsData
     */
    public void pushData(Collection<K> statsData) {
        if (stopped) {
            return;
        }
        for(K data : statsData) {
            pushData(data);
        }
    }   
    
    /**
     * @param data
     */
    public void pushData(K data) {
        if (stopped) {
            return;
        }
        boolean success = queue.writeEvent(data);
        if (isCritical()) {
            // track stats
            if (success) {
                counter.increment(Type.EVENT_PROCESSED);
            } else {
                counter.increment(Type.EVENT_DISCARDED);
            }
        }
    }
    
    @Override
    public String toString() {
        return "HandlerQueueTuple [eventHandler=" + eventHandler + "]";
    }

    /**
     * Inner worker that polls data from the {@link EventQueue} and dispatches to the {@link TurbineDataHandler}
     */
    private class PerHandlerDispatcher implements Worker {
        
        private final EventQueue<K> queue;
        private final TurbineDataHandler<K> eventHandler;
        
        private PerHandlerDispatcher(EventQueue<K> queue, TurbineDataHandler<K> handler) {
            this(queue, handler, 1);
        }
        private PerHandlerDispatcher(EventQueue<K> queue, TurbineDataHandler<K> handler, int divFactor) {
            this.queue = queue;
            this.eventHandler = handler;
        }

        @Override
        public void init() throws Exception {
            // do nothing
            logger.info("Per handler dispacher started for: " + eventHandler.getName());
        }

        @Override
        public void doWork() throws Exception {
            
            List<K> statsData = new ArrayList<K>();
            
            int numMisses = 0;
            boolean stopPolling = false;
            
            do {
                K data = queue.readEvent();
                if (data == null) {
                    numMisses++;
                    if (numMisses > 100) {
                        Thread.sleep(100);
                        numMisses = 0; // reset count so we can try polling again.
                    }
                } else {
                    statsData.add(data);
                    numMisses = 0;
                    stopPolling = true;
                }
            }
            while(!stopPolling);
            
            
            try {
                eventHandler.handleData(statsData);
            } catch (Exception e) {
                if(eventHandler.getCriteria().isCritical()) {
                    logger.warn("Could not publish event to event handler for " + eventHandler.getName(), e);
                }
            }
        }

        @Override
        public void cleanup() throws Exception {
            //queue.clear();
        }
    }
    
    public static class UnitTest {
        
        @Test
        public void testProcessWithMultipleThreads() throws Exception {
            
            final AtomicInteger handlerCount = new AtomicInteger(0);
            
            TurbineDataHandler<TurbineData> testHandler = new TurbineDataHandler<TurbineData>() {

                @Override
                public String getName() {
                    return "testHandler";
                }

                @Override
                public void handleData(Collection<TurbineData> stats) {
                    handlerCount.addAndGet(stats.size());
                }

                @Override
                public void handleHostLost(Instance host) {
                }

                @Override
                public PerformanceCriteria getCriteria() {
                    return new PerformanceCriteria() {

                        @Override
                        public boolean isCritical() {
                            return false;
                        }

                        @Override
                        public int getMaxQueueSize() {
                            return 10000;
                        }

                        @Override
                        public int numThreads() {
                            return 1;
                        }
                        
                    };
                }
            };
            
            final HashMap<String, Object> testAttrs = new HashMap<String, Object>();
            final Instance host = new Instance("host", "cluster", true);

            final HandlerQueueTuple<TurbineData> tuple = new HandlerQueueTuple<TurbineData>(testHandler);
            tuple.start();
            
            final AtomicBoolean stop = new AtomicBoolean(false);
            ExecutorService threadPool = Executors.newFixedThreadPool(10);
            List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
            
             for (int i=0; i<10; i++) {
                 futures.add(threadPool.submit(new Callable<Integer>() {

                    final AtomicInteger count = new AtomicInteger(0);
                    final Random random = new Random();
                    @Override
                    public Integer call() throws Exception {
                        while (!stop.get()) {
                            Collection<TurbineData> data = getRandomData();
                            tuple.pushData(data);
                            count.addAndGet(data.size());
                            Thread.sleep(50);
                        }
                        return count.get();
                    }

                    private Collection<TurbineData> getRandomData() {
                        int size = random.nextInt(10);
                        
                        List<TurbineData> list = new ArrayList<TurbineData>();
                        for (int i=0; i<size; i++) {
                            list.add(new DataFromSingleInstance(null, "type", "name", host, testAttrs, 0L));
                        }
                        return list;
                    }
                    
                }));
            }
             
             Thread.sleep(3000);
             
             stop.set(true);
             int sum = 0;
             for (Future<Integer> f : futures) {
                 sum += f.get();
             }
             
             threadPool.shutdownNow();
             Thread.sleep(2000);
             tuple.stop();
             Thread.sleep(1000);
             
             assertTrue(sum == handlerCount.get());
        }
    }
}

