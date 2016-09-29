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
package com.netflix.turbine.data;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

/**
 * EventQueue implementation using an {@link ArrayBlockingQueue}
 *
 * @param <T>
 */
public class ArrayBackedEventQueue<T> implements EventQueue<T> { 

    private ArrayBlockingQueue<T> queue;
    
    public ArrayBackedEventQueue(int size) {
        queue = new ArrayBlockingQueue<T>(size);
    }
    
    @Override
    public T readEvent() {
        return queue.poll();
    }

    @Override
    public boolean writeEvent(T event) {
        return queue.offer(event);
    }

    @Override
    public int getQueueSize() {
        return queue.size();
    }
    
    public static class UnitTest {
        
        // input
        private volatile List<String> stringCache;
        // output
        private ConcurrentHashMap<String, AtomicLong> globalMap;
        // testControl
        private volatile boolean shutdown = false;
        
        private volatile boolean producerShutdown = false;

        private ArrayBackedEventQueue<String> array;
        
        @Before
        public void before() {
            
            int cacheSize = 10;
            
            stringCache = new ArrayList<String>(cacheSize);
            for (int i=0; i<cacheSize; i++) {
                stringCache.add(UUID.randomUUID().toString());
            }

            globalMap = new ConcurrentHashMap<String, AtomicLong>();
            array = new ArrayBackedEventQueue<String>(100);
        }
        
        @Test (timeout=8000)
        public void testSingleProducerSingleConsumer() throws Exception {
           testProcess(1, 1);
        }
        
        @Test (timeout=8000)
        public void testSingleProducerMultiConsumer() throws Exception {
            testProcess(1, 10);
        }
        
        @Test (timeout=8000)
        public void testMultiProducerSingleConsumer() throws Exception {
            testProcess(10, 1);
        }
        
        @Test (timeout=8000)
        public void testMultiProducerMultiConsumer() throws Exception {
            testProcess(10, 10);
        }
        
        public void testProcess(int numProducers, int numConsumers) throws Exception {

            shutdown = false;
            producerShutdown = false;
            
            ExecutorService producerPool = Executors.newFixedThreadPool(numProducers);
            ExecutorService consumerPool = Executors.newFixedThreadPool(numConsumers);

            List<Future<Map<String, Long>>> pFutures = new ArrayList<Future<Map<String, Long>>>(numProducers);
            
            for (int i=0; i<numProducers; i++) {
                Future<Map<String, Long>> future = producerPool.submit(new Producer());
                pFutures.add(future);
            }

            for (int i=0; i<numConsumers; i++) {
                consumerPool.submit(new Consumer());
            }
            
            Thread.sleep(4*1000);
            
            shutdown = true;
            
            producerPool.shutdown();
            
            while (!producerPool.isTerminated()) {
                Thread.sleep(100);
            }
            
            producerShutdown = true; //signal that all producers have finished. This will tell the consumers to stop
            
            consumerPool.shutdownNow();
            while (!consumerPool.isTerminated()) {
                Thread.sleep(100);
            }
            
            Map<String, Long> expected = new HashMap<String, Long>();
            
            for(Future<Map<String, Long>> future : pFutures) { 
                
                Map<String, Long> result = future.get();
                //System.out.println("Expected: " + result);
                for (String key : result.keySet()) {
                    
                    Long count = expected.get(key);
                    if (count == null) {
                        count = new Long(0);
                    }
                    count += result.get(key);
                    expected.put(key, count);
                }
            }

            // check with expected output
            for (String key : expected.keySet()) {
                Long expectedCount = expected.get(key);
                Long resultCount = globalMap.get(key).get();
                
                assertTrue(expectedCount.longValue() == resultCount.longValue());
            }
            
            System.out.println("Producer count: " + producerCount.get());
            System.out.println("Consumer count: " + consumerCount.get());
            assertTrue(producerCount.get() == consumerCount.get());;

        }
        
        private AtomicLong producerCount = new AtomicLong(0);
        
        private class Producer implements Callable<Map<String, Long>> {
            
            private final Map<String, Long> result = new HashMap<String, Long>();
            private final Random random = new Random();
            
            private int failures = 0;
            
            @Override
            public Map<String, Long> call() throws Exception {
                
                while(!shutdown) {
                    
                    try {
                    int index = random.nextInt(stringCache.size());
                    String randomString = stringCache.get(index);
                    boolean success = array.writeEvent(randomString);

                    if (success) {
                        producerCount.incrementAndGet();
                        Long count = result.get(randomString);
                        if (count == null) {
                            count = new Long(0L);
                        }
                        count++;
                        result.put(randomString, count);
                    } else {
                        failures++;
                        if (failures > 100) {
                            Thread.sleep(100);
                        }
                    }
                    
                    } catch (Throwable t) {
                        System.out.println("ttt" + t.getMessage());
                        throw new RuntimeException(t);
                    }
                }
                
                return result; 
            }
        }

        private AtomicLong consumerCount = new AtomicLong(0);
        
        private class Consumer implements Callable<Void> {

            @Override
            public Void call() throws Exception {
                
                boolean stop = false;
                
                while (!stop) {
                    
                    try {
                    String key = array.readEvent();
                    if (key != null) {
                        
                        consumerCount.incrementAndGet();
                        //System.out.println("Key: " + key);
                        AtomicLong keyCount = globalMap.get(key);
                        if (keyCount == null) {
                            globalMap.putIfAbsent(key, new AtomicLong(0));
                        }
                        keyCount = globalMap.get(key);
                        keyCount.incrementAndGet();
                        
                    } else {
                        if (producerShutdown) {
                            stop = true;
                        }
                    }
                    } catch(Throwable t) {
                        System.out.println("Throwable caught: " + t.getMessage());
                        throw new RuntimeException(t);
                    }
                }
                return null; 
            }
        }
    }
 
}
