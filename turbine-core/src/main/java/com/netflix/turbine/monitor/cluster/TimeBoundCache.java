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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;

/**
 * A simple cache with an expiry on the fields. There is a janitor thread that runs every x millis and cleans out entries that are not needed. 
 * Note that this is NOT an LRU cache, in the sense that the cache itself is unbounded, hence to be used with care!
 * Also entries are not expired on basis of their use, but on basis of when they were last put into the cache. 
 * 
 * @param <T>
 */
class TimeBoundCache<T> {

    private static final Logger logger = LoggerFactory.getLogger(TimeBoundCache.class);
    
    private final String name; 
    private final ConcurrentHashMap<T, AtomicLong> cache;
    private final Timer timer; 
    private final AtomicBoolean started = new AtomicBoolean(false);
    
    private static final DynamicIntProperty expirySeconds = DynamicPropertyFactory.getInstance().getIntProperty("turbine.TimeBoundCache.expirySeconds", 15);
    private static final DynamicIntProperty pollDelaySeconds = DynamicPropertyFactory.getInstance().getIntProperty("turbine.TimeBoundCache.pollDelaySeconds", 5);
    
    TimeBoundCache(String name) {
        this(name, 300*1000);
    }

    private TimeBoundCache(String name, int sleepMillisForJanitor) {
        this.name = name;
        this.cache = new ConcurrentHashMap<T, AtomicLong>();
        this.timer = new Timer();
    }

    void startCache() throws Exception {
        if (!started.get()) {
            TimerTask task = new CacheJanitor();
            timer.schedule(task, pollDelaySeconds.get()*1000, pollDelaySeconds.get()*1000);
            started.set(true);
        }
    }

    void stopCache() {
        timer.cancel();
        started.set(false);
    }
    
    void put(T cacheKey) {
        
        Long currentTime = System.currentTimeMillis();
        while(true) {
            AtomicLong prevKeyTime = cache.get(cacheKey);
            if (prevKeyTime != null) {
                Long prevTime = prevKeyTime.get();
                if(currentTime < prevTime) {
                    // someone beat me to it, just return
                    return;
                }
                // ok, the value is older, let's try to refresh it
                boolean success = prevKeyTime.compareAndSet(prevTime, currentTime);
                if (success) {
                    // we won, simply return .. our work is done here
                    return;
                }
                // else we did not win, try again. ... 
                // may not need to try again here, since time always marches fwd and the next thread would have set something to higher here
            } else {
                // this key is not present in the map, add it
                AtomicLong prevTimeObj = cache.putIfAbsent(cacheKey, new AtomicLong(currentTime));
                if(prevTimeObj == null) {
                    // we won, our work is done here ... .return
                    return;
                } else {
                    // someone beat us to it, try the whole thing again
                }
            }
        }
    }
    
    boolean lookup(T cacheKey) {
        AtomicLong timestamp = cache.get(cacheKey);
        return timestamp != null;
    }

    String print() {
        return cache.keySet().toString();
    }
    
    private class CacheJanitor extends TimerTask {
        
        @Override
        public void run() {
            
            if (logger.isDebugEnabled()) {
                logger.debug("Checking for stale entries in cache for cluster: " + name);
            }
            
            Long now = System.currentTimeMillis();
            List<T> markForDeletion = new ArrayList<T>();
            
            for (T key : cache.keySet()) {
                long delay = now - cache.get(key).get();
                if (delay > (expirySeconds.get()*1000)) {
                    markForDeletion.add(key);
                }
            }
            
            for (T key : markForDeletion) {
                cache.remove(key);
            }
        }
    }
    
    public static class UnitTest {
        
        private static final List<String> randomStrings = new ArrayList<String>();
        private static final int numStrings = 100;
        private static final TimeBoundCache<String> cache = new TimeBoundCache<String>("test", 100);  // keep keys around for 2 seconds and check every 200 ms
        
        @Test
        public void testCache() throws Exception {
            
            ConfigurationManager.getConfigInstance().setProperty("turbine.TimeBoundCache.expirySeconds", 2);
            ConfigurationManager.getConfigInstance().setProperty("turbine.TimeBoundCache.pollDelaySeconds", 2);

            int numThreads = 100;
            ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
            
            for (int i=0; i<numStrings; i++) {
                String randomString = UUID.randomUUID().toString();
                randomStrings.add(randomString);
                cache.put(randomString);
            }
            
            List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>(numThreads);
            for (int i=0; i<numThreads; i++) {
                futures.add(threadPool.submit(new TestWorker(true)));
            }

            cache.startCache();
            
            // let everyone hit the cache for a while
            Thread.sleep(1000);
            
            for(Future<Boolean> future : futures) {
                assertTrue(future.get());
            }
            
            // let the cache entries expire
            Thread.sleep(2500);
         
            futures.clear();
            for (int i=0; i<numThreads; i++) {
                futures.add(threadPool.submit(new TestWorker(false)));
            }
            
            // let everyone get a cache miss
            Thread.sleep(1000);

            for(Future<Boolean> future : futures) {
                assertTrue(future.get());
            }
            
            // not repopulate the cache
            for(String string : randomStrings) {
                cache.put(string);
            }
            
            futures.clear();
            for (int i=0; i<numThreads; i++) {
                futures.add(threadPool.submit(new TestWorker(true)));
            }
            
            // wait for everyone to get a cache hit again
            Thread.sleep(1000);

            for(Future<Boolean> future : futures) {
                assertTrue(future.get());
            }

            cache.stopCache();
        }
        
        private static class TestWorker implements Callable<Boolean> {

            private final boolean expected; 
            
            private TestWorker(boolean expectHit) {
                expected = expectHit;
            }
            
            @Override
            public Boolean call() throws Exception {
                    
                Random random = new Random();
                
                for(int i=0; i<1000; i++) {
                    int index = random.nextInt(numStrings);
                    String randomString = randomStrings.get(index);
                    boolean hit = cache.lookup(randomString);
                    if (hit != expected) {
                        return false; // failure
                    }
                }
                return true;  // success
            }
        }
    }
}
