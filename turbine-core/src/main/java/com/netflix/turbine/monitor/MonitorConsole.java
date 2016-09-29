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
package com.netflix.turbine.monitor;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.turbine.data.DataFromSingleInstance;
import com.netflix.turbine.data.TurbineData;
import com.netflix.turbine.monitor.cluster.ClusterMonitor;
import com.netflix.turbine.monitor.instance.InstanceMonitor;

/**
 * Class that represents a collection of {@link TurbineDataMonitor} objects for discovery purposes. 
 * <p>This can be used by {@link ClusterMonitor}s to manage their individual {@link InstanceMonitor} objects. 
 * <p>One can also use the console to discover the monitor and then connect to it to receive it's data stream. 
 * 
 * @see InstanceMonitor
 * @param <K>
 */
public class MonitorConsole<K extends TurbineData> {
   
    // the underlying registry
    private ConcurrentHashMap<String, TurbineDataMonitor<K>> registry = new ConcurrentHashMap<String, TurbineDataMonitor<K>>();
    
    // stats
    private AtomicInteger monitorCount = new AtomicInteger(0);
    
    /**
     * No-arg constructor
     */
    public MonitorConsole() {
    }
    
    /**
     * @return Collection<{@link TurbineDataMonitor}<K>>
     */
    public Collection<TurbineDataMonitor<K>> getAllMonitors() {
        return registry.values();
    }
    
    @Monitor(name = "numMonitors", type = DataSourceType.GAUGE)
    int getNumberOfMonitors() {
    	return monitorCount.get();
    }
    
    /**
     * Helper method that finds or registers the specified monitor in a thread safe manner. 
     * Callers of this method MUST use the returned monitor, in case another thread beats them
     * to registering the same monitor.
     * 
     * @param monitor
     * @return {@link TurbineDataMonitor}<K>
     */
    public TurbineDataMonitor<K> findOrRegisterMonitor(TurbineDataMonitor<K> monitor) {
        
        TurbineDataMonitor<K> previous = registry.putIfAbsent(monitor.getName(), monitor);
        if(previous != null) {
            return previous;
        } else {
        	monitorCount.incrementAndGet();
            return monitor;
        }
    }

    /**
     * Cheap helper to find a monitor by it's name. Useful for callers that do not care about constructing the 
     * monitor themselves, and do not care if the monitor is null. 
     * @param name
     * @return StatsMonitor<K>
     */
    public TurbineDataMonitor<K> findMonitor(String name) {
        return registry.get(name);
    }

    /**
     * Helper method to clean out the monitor from the console. Note that stopping the monitor is the responsibility of the caller.
     * @param name
     */
    public TurbineDataMonitor<K> removeMonitor(String name) {
    	
    	TurbineDataMonitor<K> prev = registry.remove(name);
    	if(prev != null) {
    		monitorCount.decrementAndGet();
    	}
        return registry.remove(name);
    }
    
    @Override
    public String toString() {
        return registry.keySet().toString();
    }
    
    @SuppressWarnings("unchecked")
    public static class UnitTest {
        
        TurbineDataMonitor<DataFromSingleInstance> monitor;
        
        private void doTheMockMagic() throws Exception {
            monitor = mock(TurbineDataMonitor.class);
            when(monitor.getName()).thenReturn("publisher");
        }

        private TurbineDataMonitor<DataFromSingleInstance> getMockMonitor() throws Exception {
            TurbineDataMonitor<DataFromSingleInstance> monitorNew = mock(TurbineDataMonitor.class);
            when(monitorNew.getName()).thenReturn("publisher");
            return monitorNew;
        }
        
        @Test
        public void testFindOrStartMonitorIsIdempotent() throws Exception {
        
            doTheMockMagic();
            
            MonitorConsole<DataFromSingleInstance> console = new MonitorConsole<DataFromSingleInstance>();
            assertNull(console.findMonitor("publisher"));
            
            TurbineDataMonitor<DataFromSingleInstance> newMonitor = console.findOrRegisterMonitor(monitor);
            newMonitor.startMonitor();
            
            Thread.sleep(200);
            
            assertNotNull(console.findMonitor("publisher"));
            assertTrue(newMonitor == monitor); // check that their references are the same
            
            monitor.stopMonitor();
        }

        @Test
        public void testFindOrStartMonitorIsThreadSafe() throws Exception {
            
            class Pair {
                private TurbineDataMonitor<DataFromSingleInstance> origMonitor; 
                private TurbineDataMonitor<DataFromSingleInstance> returnMonitor; 
                private Pair(TurbineDataMonitor<DataFromSingleInstance> origMonitor, TurbineDataMonitor<DataFromSingleInstance> returnMonitor) {
                    this.origMonitor = origMonitor;
                    this.returnMonitor = returnMonitor;
                }
            }
            doTheMockMagic();
            
            List<Future<Pair>> results = new ArrayList<Future<Pair>>();
            ExecutorService executor = Executors.newFixedThreadPool(200);
            final MonitorConsole<DataFromSingleInstance> console = 
                    new MonitorConsole<DataFromSingleInstance>();
            for(int i=0; i<200; i++) {
                
                results.add(executor.submit(new Callable<Pair>() {

                    @Override
                    public Pair call() throws Exception {
                        // start monitor - only one should win
                        TurbineDataMonitor<DataFromSingleInstance> origMonitor = getMockMonitor();
                        TurbineDataMonitor<DataFromSingleInstance> returnMonitor = console.findOrRegisterMonitor(origMonitor); 
                        returnMonitor.startMonitor();
                        return new Pair(origMonitor, returnMonitor);
                    }
                }));
            }
            
            TurbineDataMonitor<DataFromSingleInstance> runningMonitor = results.iterator().next().get().returnMonitor;
            Thread.sleep(200);
            
            for(Future<Pair> pair : results) {
                TurbineDataMonitor<DataFromSingleInstance> returnMonitor = pair.get().returnMonitor;
                assertTrue(returnMonitor == runningMonitor);
            }
            
            runningMonitor.stopMonitor();
        }
    }
}