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
package com.netflix.turbine.utils;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

/**
 * A simple implementation of a map of maps, where each inner map is a concurrent hashmap
 * and the outer map is also a concurrent hashmap
 * 
 * @param <K>
 * @param <V>
 */
public class ThreadSafeMultiMap<K,V> {
    
    // the map of maps
    private ConcurrentHashMap<K, ConcurrentHashMap<V,V>> mapOfMaps; 
    
    /**
     * Constructor 
     */
    public ThreadSafeMultiMap() {
        mapOfMaps = new ConcurrentHashMap<K, ConcurrentHashMap<V, V>>();
    }
    
    /**
     * Put the specified key value, only if both K,V do not previously exist
     *  
     * @param key
     * @param value
     * @return V if V previously existed
     */
    public V putIfAbsent(K key, V value) {
        
        ConcurrentHashMap<V,V> innerMap = mapOfMaps.get(key);
        
        if (innerMap == null) {
            innerMap = new ConcurrentHashMap<V,V>();
            ConcurrentHashMap<V,V> oldMapValue = mapOfMaps.putIfAbsent(key, innerMap);
            // another thread beat us to it. move on
            if (oldMapValue != null) {
                innerMap = oldMapValue;
            }
        }
        
        // Now repeat essentially the same procedure for the value V in the inner hashmap
        V oldValue = innerMap.get(value);
        if (oldValue == null) {
            oldValue = innerMap.putIfAbsent(value, value);
            // here we may return the actual oldValue if it existed, or null if this was the first put for this value
            // this is the intended behavior
            return oldValue;
        } else {
            // value was already there, hence return it to the calling program
            return oldValue;
        }
    }
    
    /**
     * Best effort attempt to remove the K,V from the map of maps
     * 
     * @param key
     * @param value
     * @return V if V was actually removed
     */
    public V remove(K key, V value) {
        
        ConcurrentHashMap<V,V> innerMap = mapOfMaps.get(key);
        
        // note that there is a race here, where the key could be created at the same time that we check
        // and find that the inner map is not present.
        if (innerMap == null) {
            return null; // even the inner map is not present, and hence the inner value is not present
        }
        return innerMap.remove(value);
    }

    /**
     * Returns all keys associated with the map 
     * @return Set<K>
     */
    public Set<K> keySet() {
        return mapOfMaps.keySet();
    }
    
    /**
     * Return the list of values that map to the key K
     * @param key
     * @return List<V>
     */
    public List<V> getValues(K key) {
        
        ConcurrentHashMap<V,V> innerMap = mapOfMaps.get(key);
        if (innerMap == null) {
            return null;
        }
        return new ArrayList<V>(innerMap.values());
    }

    /**
     * Return value V that matches key K
     * @param key
     * @param value
     * @return V or null if V is missing
     */
    public V getValue(K key, V value) {

        ConcurrentHashMap<V,V> innerMap = mapOfMaps.get(key);
        if (innerMap == null) {
            return null;
        }
        return innerMap.get(value);
    }
    
    public static class UnitTest {
        
        private ThreadSafeMultiMap<String, Integer> multiMap; 
        private List<String> input;
        private volatile boolean stopped;
        private ExecutorService producerPool;
        
        private int numKeys = 100;
        private int numProducers = 100;
        
        @Before
        public void before() {
            multiMap = new ThreadSafeMultiMap<String, Integer>();
            input = new ArrayList<String>(numKeys);
            for (int i=0; i<numKeys; i++) {
                input.add(UUID.randomUUID().toString());
            }
            stopped = false;
            producerPool = Executors.newFixedThreadPool(numProducers);
        }
        
        @Test
        public void testAddKeysAtRandom() throws Exception {
            
            List<Future<Map<String, Set<Integer>>>> futures = new ArrayList<Future<Map<String, Set<Integer>>>>();
            
            for (int i=0; i<numProducers; i++) {
                futures.add(producerPool.submit(new AddRandomKeyValue()));
            }
            
            Thread.sleep(4000);
            
            stopped = true; 
            
            producerPool.shutdown();
            
            while(!producerPool.isTerminated()) {
                Thread.sleep(100);
            }
            
            Map<String, Set<Integer>> totalResult = new HashMap<String, Set<Integer>>();
            for (Future<Map<String, Set<Integer>>> result : futures) {
                
                Map<String, Set<Integer>> resultMap = result.get();
                for (String key : resultMap.keySet()) {
                    
                    Set<Integer> resultIntegers = resultMap.get(key);
                    Set<Integer> totalIntegers = totalResult.get(key);
                    
                    if (totalIntegers == null) {
                        totalIntegers = new HashSet<Integer>();
                        totalResult.put(key, totalIntegers);
                    }
                    
                    totalIntegers.addAll(resultIntegers);
                }
            }
            
            
            for (String key : totalResult.keySet()) {
                
                Set<Integer> expected = totalResult.get(key);
                Set<Integer> result = new HashSet<Integer>(multiMap.getValues(key));
             
                assertEquals(expected, result);
            }
        }
        
        @Test
        public void testRemoveKeysRandomly() throws Exception {
            
            // pre populate the map
            for (String key : input) {
                for (int i=0; i<numKeys; i++) {
                    multiMap.putIfAbsent(key, i);
                }
            }
            
            // now launch threads that will remove keys at random
            List<Future<Map<String, Integer>>> futures = new ArrayList<Future<Map<String, Integer>>>();
            
            for (int i=0; i<numProducers; i++) {
                futures.add(producerPool.submit(new RemoveRandomKeyValue()));
            }
            
            Map<String, Integer> totalResult = new HashMap<String, Integer>();
            
            for (Future<Map<String, Integer>> result : futures) {
                
                Map<String, Integer> resultMap = result.get();
                
                for (String key : resultMap.keySet()) {
                    
                    Integer resultCount = resultMap.get(key);
                    Integer totalCount = totalResult.get(key);
                    
                    if (totalCount == null) {
                        totalCount = 0;
                    }
                    totalCount += resultCount;
                    totalResult.put(key, totalCount);
                }
            }
            
            
            for (String key : totalResult.keySet()) {
                assertTrue(numKeys == totalResult.get(key).intValue());
            }
            
            producerPool.shutdownNow();
        }
        
        private class AddRandomKeyValue implements Callable<Map<String, Set<Integer>>> {

            private Map<String, Set<Integer>> result = new HashMap<String, Set<Integer>>();
            private Random random = new Random();
            
            @Override
            public Map<String, Set<Integer>> call() throws Exception {
                
                while (!stopped) {
                    
                    String randomString = input.get(random.nextInt(numKeys));
                    Integer randomInt = random.nextInt(numKeys);
                    
                    multiMap.putIfAbsent(randomString, randomInt);
                    
                    Set<Integer> set = result.get(randomString);
                    if (set == null) {
                        set = new HashSet<Integer>();
                        result.put(randomString, set);
                    }
                    
                    set.add(randomInt);
                }
                
                return result;
            }
        }
        
        private class RemoveRandomKeyValue implements Callable<Map<String, Integer>> {

            private Map<String, Integer> result = new HashMap<String, Integer>();
            
            @Override
            public Map<String, Integer> call() throws Exception {
                
                for (String key : multiMap.keySet()) {
                    
                    int count = 0;
                    result.put(key, count);
                    
                    Random random = new Random();
                    boolean done = false;
                    
                    do {
                        List<Integer> integers = multiMap.getValues(key);
                        done = integers.size() == 0;
                        
                        Integer removed = multiMap.remove(key, random.nextInt(numKeys));
                        if (removed != null) {
                            result.put(key, result.get(key) + 1);
                        }
                        
                    } while(!done);
                }                
                return result;
            }
        }
    }
}
