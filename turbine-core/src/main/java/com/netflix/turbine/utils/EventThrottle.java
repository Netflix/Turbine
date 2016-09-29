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
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;

/**
 * Utility class for throttling an event stream as per a size and time threshold. 
 *
 * @param <K>
 */
public class EventThrottle<K> {
    
    private AtomicInteger eventCount = new AtomicInteger(0);
    private volatile long lastEventTime = -1; 
    
    private final DynamicIntProperty eventFlushThreshold;
    private final DynamicIntProperty eventFlushDelayMillis;
    
    /**
     * @param eventThreshold  : how many events to wait for
     * @param flushDelay      : how long to wait for
     */
    public EventThrottle(DynamicIntProperty eventThreshold, DynamicIntProperty flushDelay) {
        this.eventFlushThreshold = eventThreshold;
        this.eventFlushDelayMillis = flushDelay;
    }

    /**
     * Check if the events must be throttled or not.
     * @param events
     * @return true/false
     */
    public boolean throttle(Collection<K> events) {
        return throttleEvents(events.size());
    }

    public boolean throttle(K event) {
        return throttleEvents(1);
    }

    private boolean throttleEvents(int size) {
        
        eventCount.addAndGet(size);
        
        boolean succeeded = false;
        while(!succeeded) {
            
            long delay = (lastEventTime > 0 ) ? System.currentTimeMillis() - lastEventTime : eventFlushDelayMillis.get(); 
            int count = eventCount.get();
            
            if(delay < eventFlushDelayMillis.get() && count < eventFlushThreshold.get()) {
                break; // no need to push event just as yet, just exit from here. Hence throttle
            }
            count = eventCount.get(); // do this again, since it increases our chances of winning
            succeeded = eventCount.compareAndSet(count, 0); // his thread wins and is convinced that the count is over the threshold
        }
        
        if(succeeded) {
            lastEventTime = System.currentTimeMillis(); // reset the time to now
            return false; // do not throttle, let the data through
        } else {
            return true; // someone else beat this thread in sending some events out, hence throttle this value. 
        }
    }
    
    public static class UnitTest {
        
        private Random random = new Random();
        
        @Test
        public void testThrottleOnCountsOnly() throws Exception {
            
            DynamicIntProperty countThreshold = DynamicPropertyFactory.getInstance().getIntProperty("foo", 1000);
            DynamicIntProperty delayThreshold = DynamicPropertyFactory.getInstance().getIntProperty("bar", 10);
            
            EventThrottle<Integer> throttle = new EventThrottle<Integer>(countThreshold, delayThreshold);
            
            boolean shouldThrottle = throttle.throttle(getRandomIntegers(100));
            assertFalse(shouldThrottle);  // first time, we let this through
            
            shouldThrottle = throttle.throttle(getRandomIntegers(300));
            assertTrue(shouldThrottle);

            shouldThrottle = throttle.throttle(getRandomIntegers(200));
            assertTrue(shouldThrottle);

            shouldThrottle = throttle.throttle(getRandomIntegers(400));
            assertTrue(shouldThrottle);
            
            shouldThrottle = throttle.throttle(getRandomIntegers(200));
            assertFalse(shouldThrottle);
            
            // now the count got reset. Try again. 
            shouldThrottle = throttle.throttle(getRandomIntegers(400));
            assertTrue(shouldThrottle);
            
            shouldThrottle = throttle.throttle(getRandomIntegers(300));
            assertTrue(shouldThrottle);

            shouldThrottle = throttle.throttle(getRandomIntegers(310));
            assertFalse(shouldThrottle);
        }
        
        @Test
        public void testThrottleOnTimeOnly() throws Exception {
            
            DynamicIntProperty countThreshold = DynamicPropertyFactory.getInstance().getIntProperty("foo", 100000);
            DynamicIntProperty delayThreshold = DynamicPropertyFactory.getInstance().getIntProperty("bar", 100);
            
            EventThrottle<Integer> throttle = new EventThrottle<Integer>(countThreshold, delayThreshold);
            
            boolean shouldThrottle = throttle.throttle(getRandomIntegers(100));
            assertFalse(shouldThrottle);  // first time, we let this through
            
            shouldThrottle = throttle.throttle(getRandomIntegers(300));
            assertTrue(shouldThrottle);

            Thread.sleep(120);
            
            // now let this through
            shouldThrottle = throttle.throttle(getRandomIntegers(200));
            assertFalse(shouldThrottle);

            shouldThrottle = throttle.throttle(getRandomIntegers(400));
            assertTrue(shouldThrottle);
            
            shouldThrottle = throttle.throttle(getRandomIntegers(200));
            assertTrue(shouldThrottle);
            
            Thread.sleep(120);
            
            // again let this through
            shouldThrottle = throttle.throttle(getRandomIntegers(400));
            assertFalse(shouldThrottle);
        }
        
        @Test
        public void testThrottleOnTimeAndCount() throws Exception {
            
            DynamicIntProperty countThreshold = DynamicPropertyFactory.getInstance().getIntProperty("foo", 1000);
            DynamicIntProperty delayThreshold = DynamicPropertyFactory.getInstance().getIntProperty("bar", 100);
            
            EventThrottle<Integer> throttle = new EventThrottle<Integer>(countThreshold, delayThreshold);
            
            boolean shouldThrottle = throttle.throttle(getRandomIntegers(100));
            assertFalse(shouldThrottle);  // first time, we let this through
            
            shouldThrottle = throttle.throttle(getRandomIntegers(300));
            assertTrue(shouldThrottle);

            Thread.sleep(120);
            
            // now let this through
            shouldThrottle = throttle.throttle(getRandomIntegers(200));
            assertFalse(shouldThrottle);
            
            shouldThrottle = throttle.throttle(getRandomIntegers(400));
            assertTrue(shouldThrottle);
            
            // count went over threshold, let this through
            shouldThrottle = throttle.throttle(getRandomIntegers(700));
            assertFalse(shouldThrottle);
            
            Thread.sleep(120);
            
            // again let this through
            shouldThrottle = throttle.throttle(getRandomIntegers(400));
            assertFalse(shouldThrottle);
        }
        
        private List<Integer> getRandomIntegers(int n) {
            
            List<Integer> list = new ArrayList<Integer>(n);
            for (int i=0; i<n; i++) {
                list.add(random.nextInt());
            }
            return list;
        }
        
    }
}
