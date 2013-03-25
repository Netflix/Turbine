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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.util.MinimalPrettyPrinter;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.monitor.TurbineDataMonitor;
import com.netflix.turbine.monitor.cluster.AggregateClusterMonitor;
import com.netflix.turbine.monitor.instance.InstanceMonitor;
import com.netflix.turbine.utils.AppDeploymentConfig;

/**
 * Class that extends TurbineData and represents the aggregate view of data
 * across a cluster of Instances for a given <b>name</b> and <b>type</b>.
 * <p>
 * The {@link AggregateClusterMonitor} aggregates all
 * {@link DataFromSingleInstance} that it receives from various
 * {@link InstanceMonitor}s and represents the combined sum of the data
 * attributes in this class.
 * </p>
 */
public class AggDataFromCluster extends TurbineData {

    private static final Logger logger = LoggerFactory.getLogger(AggDataFromCluster.class);

    private static final String reportingHosts = "reportingHosts";

    /* used to track data/counters/etc for each APIInstance that reports */
    private ConcurrentHashMap<String, HostDataHolder> reportingHostsWithLastData = new ConcurrentHashMap<String, HostDataHolder>();

    // data attrs
    private ConcurrentHashMap<String, AtomicLong> numericAttributes = new ConcurrentHashMap<String, AtomicLong>();
    private ConcurrentHashMap<String, StringDataValue> stringAttributes = new ConcurrentHashMap<String, StringDataValue>();
    private ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicLong>> nestedMapAttributes = new ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicLong>>();

    private final ObjectReader objectReader; 
    private final ObjectWriter objectWriter;
    
    /**
     * @param monitor
     * @param type
     * @param name
     */
    public AggDataFromCluster(TurbineDataMonitor<AggDataFromCluster> monitor, String type, String name) {
        super(monitor, type, name);
        ObjectMapper objectMapper = new ObjectMapper();
        objectReader = objectMapper.reader(Map.class);
        objectWriter = objectMapper.prettyPrintingWriter(new MinimalPrettyPrinter());
    }

    @Override
    public HashMap<String, Long> getNumericAttributes() {
        /* copy data from the AtomicLongs and ConcurrentHashmap to a static version */
        HashMap<String, Long> values = new HashMap<String, Long>();
        for (String attributeName : numericAttributes.keySet()) {
            AtomicLong nValue = numericAttributes.get(attributeName);
            if (nValue != null) {
                values.put(attributeName, nValue.get());
            }
        }
        return values;
    }

    @Override
    public HashMap<String, String> getStringAttributes() {
        /* copy data from the StringDataValue and ConcurrentHashmap to a static version */
        HashMap<String, String> values = new HashMap<String, String>();
        for (String attributeName : stringAttributes.keySet()) {
            StringDataValue sValue = stringAttributes.get(attributeName);
            if (sValue != null) {
                values.put(attributeName, sValue.getValue());
            }
        }
        return values;
    }

    @Override
    public HashMap<String, Map<String, ? extends Number>> getNestedMapAttributes() {
        /* copy data from the StringDataValue and ConcurrentHashmap to a static version */
        HashMap<String, Map<String, ? extends Number>> values = new HashMap<String, Map<String, ? extends Number>>();
        for (String nestedAttrName : nestedMapAttributes.keySet()) {
            Map<String, Long> nestedMap = new HashMap<String, Long>();
            ConcurrentHashMap<String, AtomicLong> concurrentMap = nestedMapAttributes.get(nestedAttrName);
            for (String attrName : concurrentMap.keySet()) {
                nestedMap.put(attrName, concurrentMap.get(attrName).longValue());
            }
            values.put(nestedAttrName, nestedMap);
        }
        return values;
    }

    /**
     * How many hosts have reported to this aggregate data.
     * 
     * @return int: num hosts reporting data
     */
    public int getReportingHostsCount() {
        return reportingHostsWithLastData.keySet().size();
    }

    /**
     * Called after data is aggregated from a given InstanceMonitor
     */
    public void performPostProcessing() {
        // set the reporting host count for this data
        Long sum = 0L;
        for (HostDataHolder holder : reportingHostsWithLastData.values()) {
            sum += holder.numReportingHosts.get();
        }
        numericAttributes.put(reportingHosts, new AtomicLong(sum));
    }

    public String getReportingDataDebug() {
        StringBuilder sb = new StringBuilder();
        Long sum = 0L; 
        for (String hostname : reportingHostsWithLastData.keySet()) {
            sb.append(" " + hostname);
            HostDataHolder holder = reportingHostsWithLastData.get(hostname);
            sb.append("= " + holder.numReportingHosts.get());
            sb.append(", " + (System.currentTimeMillis() - holder.lastEventTime.get()) + "ms");
            sum += holder.numReportingHosts.get();
        }
        sb.append(" Total: " + sum);
        return sb.toString();
    }
    
    /**
     * Update the cluster data with the data from this single host.
     * <p>
     * If this host already added data in the past it will first remove that data (subtract) and then add the newly updated data.
     * </p>
     * @param data
     */
    public void addStatsDataFromSingleServer(DataFromSingleInstance data) {
        
        HostDataHolder historicalDataHolder = reportingHostsWithLastData.get(data.getHost().getHostname());
        if (historicalDataHolder == null) {

            historicalDataHolder = reportingHostsWithLastData.putIfAbsent(data.getHost().getHostname(), new HostDataHolder());
            // if it's not null that means another thread beat us to setting it and we got it back
            if (historicalDataHolder == null) {
                // this means we created it and need to retrieve it again
                historicalDataHolder = reportingHostsWithLastData.get(data.getHost().getHostname());
            }
        }
        
        // Record the last time the data was touched, which is NOW
        historicalDataHolder.lastEventTime.set(System.currentTimeMillis());
        
        // Two types of values ...
        // 1) numerical that we are summing together
        // 2) strings that we want to know the values across the cluster and if they differ show how many hosts are reporting the value
        DataFromSingleInstance historical = historicalDataHolder.lastData.get();

        if (logger.isDebugEnabled()) {
            long latency = System.currentTimeMillis() - data.getCreationTime();
            // check if latency > 1 (that or smaller means the time didn't come from the actual data stream of the instance but is the default we set when we can't get it from the stream)
            if (latency > 1 && logger.isDebugEnabled()) {
                logger.debug("Latency on SingleInstanceData: " + latency);
            }
        }

//      /* numeric attributes */
      aggregateNumericMap(data.getNumericAttributes(), /** the source attrs */ 
                          numericAttributes, /** the target attrs */
                          (historical != null) ? historical.getNumericAttributes() : null /** historical attrs*/);

//    /* nested map attributes */
      if (data.getNestedMapAttributes().size() > 0) {
          
          for (String nestedMapKey : data.getNestedMapAttributes().keySet()) {
          
              // find the nestedMap. If it does not exist, then create one in a thread safe way
              ConcurrentHashMap<String, AtomicLong> aggNestedMap = nestedMapAttributes.get(nestedMapKey);
              if (aggNestedMap == null) {
                  aggNestedMap = new ConcurrentHashMap<String, AtomicLong>();
                  nestedMapAttributes.putIfAbsent(nestedMapKey, aggNestedMap);
                  aggNestedMap = nestedMapAttributes.get(nestedMapKey);
              }
          
              Map<String, ? extends Number> sourceMap = data.getNestedMapAttributes().get(nestedMapKey);
              Map<String, ? extends Number> historicalNestedMap = null;
              if (historical != null) {
                  HashMap<String, Map<String, ? extends Number>> historicalNestedMapAttrs = historical.getNestedMapAttributes();
                  if (historicalNestedMapAttrs != null) {
                      historicalNestedMap = historicalNestedMapAttrs.get(nestedMapKey); // can be null
                  }
              }
              
              aggregateNumericMap(sourceMap, /** the source attrs */ 
                                  aggNestedMap, /** the target attrs */
                                  historicalNestedMap /** historical attrs*/);
          }
      }
      
        /* string attributes */
        HashMap<String, String> sAttrs = data.getStringAttributes();
        for (String attributeName : sAttrs.keySet()) {
      
            StringDataValue stringValue = stringAttributes.get(attributeName);
            if (stringValue == null) {
                // it doesn't exist so add this value
                stringAttributes.putIfAbsent(attributeName, new StringDataValue());
                // now retrieve it again so we can add to it
                stringValue = stringAttributes.get(attributeName);
            }

            String historicalStringValue = null;
            if (historical != null) {
                historicalStringValue = historical.getStringAttributes().get(attributeName);
            }
            stringValue.setValue(sAttrs.get(attributeName), historicalStringValue);
        }

        /**
         * KNOWN BUG THAT SHOULD RARELY OCCUR: If the attribute list is different between hosts or different over time then the values for that attribute will not get updated and will be wrong.
         * <p>
         * This should be very rare as all instances being monitored should have the same attributes and the list should not dynamically change.
         * <p>
         * When it could occur is in a red/black deployment, new hosts having new or renamed attributes ... we'll just have to deal with it in those circumstances.
         */

        // store this data for history
        historicalDataHolder.lastData.set(data);
        
        // set the reporting hosts for the last data received from this host
        historicalDataHolder.numReportingHosts.set(data.getNumericAttributes().get(reportingHosts));
        
        // mark this host as having delivered data
        historicalDataHolder.hostActivityCounter.increment(StatsRollingNumber.Type.EVENT_PROCESSED);
    }

    private void aggregateNumericMap(Map<String, ? extends Number> sourceAttrs, 
                                     ConcurrentHashMap<String, AtomicLong> targetAttrs,
                                     Map<String, ? extends Number> historicalAttrs) {
        /* numeric attributes */
        
        for (String attributeName : sourceAttrs.keySet()) {

            AtomicLong sum = targetAttrs.get(attributeName);
            if (sum == null) {
                // it doesn't exist so add this value
                targetAttrs.putIfAbsent(attributeName, new AtomicLong(0));
                // now retrieve it again so we can add to it
                sum = targetAttrs.get(attributeName);
            }

            // decrement the past value (if we have a past value) and add the current new value
            int valueToAdd = sourceAttrs.get(attributeName).intValue();

            if (historicalAttrs != null) {
                Number historicalNumericalValue = historicalAttrs.get(attributeName);
                if (historicalNumericalValue != null) {
                    valueToAdd -= historicalNumericalValue.intValue();
                }
            }
            // atomically add the delta (+/-)
            sum.addAndGet(valueToAdd);
            long sumVal = sum.get();
            if(sumVal < 0) {
                sum.compareAndSet(sumVal, 0);
            }
        }
    }
    
    /**
     * This is called when a host connection gets disconnected and we remove the data from that source.
     * @param host
     */
    public void removeDataForHost(Instance host) {

        // remove it in a thread-safe manner and get what is removed (if something is removed) to then do the cleanup after
        HostDataHolder historicalDataHolder = reportingHostsWithLastData.remove(host.getHostname());
        if (historicalDataHolder == null || historicalDataHolder.lastData.get() == null) {
            // this host never reported data or it has already been removed
            return;
        }

        DataFromSingleInstance historical = historicalDataHolder.lastData.get();

        /* numeric attributes */
        removeNumericAttributes(historical.getNumericAttributes(), numericAttributes);
        
        /*nested numeric attributes */
        Map<String, Map<String, ? extends Number>> historicalNestedMapAttrs = historical.getNestedMapAttributes();
        if (historicalNestedMapAttrs != null && historicalNestedMapAttrs.keySet().size() > 0) {
            for (String nestedKey : historicalNestedMapAttrs.keySet()) {
                removeNumericAttributes(historicalNestedMapAttrs.get(nestedKey), nestedMapAttributes.get(nestedKey));
            }
        }
        
        /* string attributes */
        HashMap<String, String> sAttrs = historical.getStringAttributes();
        for (String attributeName : sAttrs.keySet()) {
            StringDataValue stringValue = stringAttributes.get(attributeName);
            if (stringValue != null) {
                stringValue.setValue(null, sAttrs.get(attributeName));
            }
        }
    }
    
    private void removeNumericAttributes(Map<String, ? extends Number> historicalAttrs, ConcurrentHashMap<String, AtomicLong> targetAttrs) {
        
        if (historicalAttrs == null || targetAttrs == null) {
            return;
        }
        
        for (String attributeName : historicalAttrs.keySet()) {
            Number numericValue = historicalAttrs.get(attributeName);

            AtomicLong sum = targetAttrs.get(attributeName);
            // if this value exists we'll delete the value of this host from it
            if (sum != null) {
                // decrement the past value
                int valueToRemove = numericValue.intValue();
                // atomically remove the value (add the negative value)
                sum.addAndGet(valueToRemove * -1);
            }
        }
    }
    
    /**
     * Simple class that represents the different sets of string values that have been received for a given key
     * and the respective value counts. 
     */
    private class StringDataValue {
        private ConcurrentHashMap<String, AtomicLong> valueCounts = new ConcurrentHashMap<String, AtomicLong>();
        
        private static final String OPEN_BRACE = "{";
        private static final String EMPTY_STRING = "";
        
        public void setValue(String newValue, String oldValue) {
            if (oldValue != null) {
                setValue(oldValue, true /** decrement = true*/);
            }
            // add the new value
            if (newValue != null) {
                setValue(newValue, false /** decrement = false*/);
            }
        }

        private void setValue(String value, boolean decrement) {
            
            if (AppDeploymentConfig.aggMode == AppDeploymentConfig.AggregatorMode.MULTI_ZONE && 
                    value.startsWith(OPEN_BRACE)) {
                try {
                    Map<String, Object> json = objectReader.readValue(value);
                    Iterator<String> keys = json.keySet().iterator();
                    
                    while (keys.hasNext()) {

                        String key = keys.next();
                        int keyCount = (Integer)json.get(key);
                        AtomicLong valueCount = valueCounts.get(key);
                        
                        if (decrement) {
                            if (valueCount != null) {
                                valueCount.addAndGet(-1*keyCount);
                            }
                        } else {
                            if (valueCount == null) {
                                // it doesn't exist so add it
                                valueCounts.putIfAbsent(key, new AtomicLong(0));
                                // retrieve the newly added one (or one added by another thread in a race)
                                valueCount = valueCounts.get(key);
                            }
                            valueCount.addAndGet(keyCount);
                        }
                    }
                } catch(Throwable t) {
                    // nothing to do here, move on
                }
            } else {
                // This is a single value, there is no json payload here
                AtomicLong valueCount = valueCounts.get(value);
                if (decrement) {
                    if (valueCount != null) {
                        valueCount.decrementAndGet();
                    }
                } else {
                    if (valueCount == null) {
                        // it doesn't exist so add it
                        valueCounts.putIfAbsent(value, new AtomicLong(0));
                        // retrieve the newly added one (or one added by another thread in a race)
                        valueCount = valueCounts.get(value);
                    }
                    valueCount.incrementAndGet();
                }   // end for if(decrement)
            } // end for if (!json)
        }

        public String getValue() {
            // if everything has reported the same value return it
            if (valueCounts.keySet().size() == 1) {
                return String.valueOf(valueCounts.keySet().toArray()[0]);
            } else {
                // we've had a mixture of values reported
                // get all values in a map first so we can get them local where they aren't being changed by other threads
                HashMap<String, Long> temp = new HashMap<String, Long>();
                for (String value : valueCounts.keySet()) {
                    temp.put(value, valueCounts.get(value).get());
                }
                // now that we have them local and can operate in a thread-safe manner, see if any of them have a count of 0 and if so, remove them
                Set<String> keys = new HashSet<String>(temp.keySet());
                for (String key : keys) {
                    if (temp.get(key) <= 0) {
                        temp.remove(key);
                    }
                }
                // now that we've cleared out all that == 0 do we only have 1 left?
                if (temp.keySet().size() == 0) {
                    // only 1 left so just return it
                    return EMPTY_STRING;
                } else if (temp.keySet().size() == 1) {
                        // only 1 left so just return it
                        return String.valueOf(temp.keySet().toArray()[0]);
                } else {
                    
                    try {
                        return objectWriter.writeValueAsString(temp);
                    } catch (JsonGenerationException e) {
                        // do nothing
                    } catch (JsonMappingException e) {
                        // do nothing
                    } catch (IOException e) {
                        // do nothing
                    }
                    return EMPTY_STRING;
                }
            }
        }
    }

    public AtomicLong putIfAbsent(String key, Long value) {
        return this.numericAttributes.putIfAbsent(key, new AtomicLong(value));
    }
    
    /**
     * Class that represents that last value seen from a given {@link Instance}.
     */
    private static class HostDataHolder {
        
        // Holder for the last data 
        public AtomicReference<DataFromSingleInstance> lastData = new AtomicReference<DataFromSingleInstance>();
        // num reporting hosts
        public final AtomicReference<Long> numReportingHosts = new AtomicReference<Long>(0L);
        // last time this was updated
        public final AtomicReference<Long> lastEventTime = new AtomicReference<Long>(0L);
        // count activity of host (whether it was reported, and how many times, in the past 10 seconds)
        public StatsRollingNumber hostActivityCounter = new StatsRollingNumber(10000, 10);
    }

    public static class UnitTest {

        @Mock
        volatile TurbineDataMonitor<AggDataFromCluster> monitor;
        @Mock
        volatile TurbineDataMonitor<DataFromSingleInstance> hostMonitor;
        
        String name = "TEST_STATS_DATA";
        
        volatile AggDataFromCluster clusterData = new AggDataFromCluster(monitor, "TEST_TYPE", name);
        
        final String[] arrayNumeric = {"A", "B", "C", "D", "E", "F"};
        final String[] arrayString = {"G", "H", "I", "J", "K", "L"};

        
        private class TestWorker implements Callable<Void> {

            final Instance host = new Instance(UUID.randomUUID().toString(), "cluster", true);
            final Random random = new Random();
            final List<String> testNumericAttrNames = Arrays.asList(arrayNumeric);
            final List<String> testStringAttrNames = Arrays.asList(arrayString);
            volatile Map<String, Long> lastNumericValues = new HashMap<String, Long>(); 
            volatile Map<String, String> lastStringValues = new HashMap<String, String>(); 
            volatile int events;
            volatile boolean stopped = false;
            
            @Override
            public Void call() throws Exception {
                
                while(!stopped) {
                    try {
                        // create a test data instance
                        DataFromSingleInstance singleInstanceData = getDataForSingleInstance();
                        // add it to the cluster data so that it can be aggregated
                        clusterData.addStatsDataFromSingleServer(singleInstanceData);
                        // record how many events were processed for this worker
                        events++;
                        // also record the last value recorded for this worker
                        lastNumericValues.clear();
                        for (String key : singleInstanceData.getNumericAttributes().keySet()) {
                            lastNumericValues.put(key, singleInstanceData.getNumericAttributes().get(key));
                        }
                        lastStringValues.clear();
                        for (String key : singleInstanceData.getStringAttributes().keySet()) {
                            lastStringValues.put(key, singleInstanceData.getStringAttributes().get(key));
                        }
                        Thread.sleep(50);
                    } catch(InterruptedException e) {
                        stopped = true;
                    }
                }
                return null;
            }
 
            /**
             * Add in test numeric and string attributes
             * @return StatsDataFromSingleInstance
             */
            private DataFromSingleInstance getDataForSingleInstance() {
                HashMap<String, Object> map = new HashMap<String, Object>();
                for (String attrName: testNumericAttrNames) {
                    map.put(attrName, random.nextInt(10));
                }
                map.put("reportingHosts", 1);
                for (String attrName: testStringAttrNames) {
                    map.put(attrName, "s" + String.valueOf(random.nextInt(2)));
                }
                
                Map<String, Integer> nestedMap = new HashMap<String, Integer>();
                nestedMap.put("N1", 10);
                nestedMap.put("N2", 11);

                Map<String, Integer> nestedMap2 = new HashMap<String, Integer>();
                nestedMap2.put("N3", 10);
                nestedMap2.put("N4", 11);
                
                map.put("nested1", nestedMap);
                map.put("nested2", nestedMap2);
                
                DataFromSingleInstance data = new DataFromSingleInstance(hostMonitor, "TEST_TYPE", name, host, map, 1);
                return data;
            }
            
            private void addLastNumericValuesToMap(Map<String, Long> theMap) {
                for (String key : lastNumericValues.keySet()) {
                    Long prevValue = theMap.get(key);
                    if(prevValue == null) {
                        prevValue = 0L;
                    }
                    theMap.put(key, prevValue + lastNumericValues.get(key));
                }
            }

            private void addLastStringValuesToMap(String attrName, Map<String, Long> theMap) {
                String attrValue  = lastStringValues.get(attrName);
                if (attrValue != null) {
                    Long prevCount = theMap.get(attrValue);
                    if(prevCount == null) {
                        prevCount = 0L;
                    }
                    theMap.put(attrValue, prevCount + 1);
                }
            }
        }

        @Test
        public void testCombineDataUsingMultipleThreads() throws Exception {
            
            int numThreads = 50;
            List<TestWorker> workers = new ArrayList<TestWorker>(numThreads);
            
            ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
            for (int i=0; i<numThreads; i++) {
                TestWorker worker = new TestWorker();
                workers.add(worker);
                threadPool.submit(worker);
            }
            
            Thread.sleep(3000);
            threadPool.shutdownNow();
            
            clusterData.performPostProcessing();
            
            boolean terminated = threadPool.awaitTermination(100, TimeUnit.MILLISECONDS);
            
            assertTrue("Threadpool NOT terminated!", terminated);
            
            System.out.println("Num reporting hosts: " + clusterData.getReportingHostsCount());
            assertEquals(numThreads, clusterData.getReportingHostsCount());
            
            int totalEvents = 0;
            Map<String, Long> totalSum = new HashMap<String, Long>();
            
            List<String> stringAttrs = Arrays.asList(arrayString);
            Map<String, Map<String, Long>> strAttrsMap = new HashMap<String, Map<String, Long>>();
            for (String strAttr : stringAttrs) {
                strAttrsMap.put(strAttr, new HashMap<String, Long>());
            }
            
            for(TestWorker worker : workers) {
                
                totalEvents += worker.events;
                
                worker.addLastNumericValuesToMap(totalSum);
                
                for (String strAttr : stringAttrs) {
                    worker.addLastStringValuesToMap(strAttr, strAttrsMap.get(strAttr));
                }
            }
            
            int recordedEvents = 0;
            for(HostDataHolder dataHolder : clusterData.reportingHostsWithLastData.values()) {
                recordedEvents += dataHolder.hostActivityCounter.getCount(StatsRollingNumber.Type.EVENT_PROCESSED);
            }
            System.out.println("Total events: " + totalEvents);
            System.out.println("Num events records: " + recordedEvents);

            totalSum.put(reportingHosts, (long) workers.size());
            
            assertEquals(totalEvents, recordedEvents);

            System.out.println(clusterData.getNumericAttributes());
            System.out.println(totalSum);

            assertEquals(totalSum, clusterData.getNumericAttributes());

            Map<String, String> formattedMap = new HashMap<String, String>();
            for (String strAttr : stringAttrs) {
                formattedMap.put(strAttr, getFormattedString(strAttrsMap.get(strAttr)));
            }
            System.out.println(clusterData.getStringAttributes());
            System.out.println(formattedMap);
            assertEquals(formattedMap, clusterData.getStringAttributes());
            
            Map<String, ? extends Number> nestedAttrs1 = clusterData.getNestedMapAttributes().get("nested1");
            assertTrue(500 == nestedAttrs1.get("N1").intValue());
            assertTrue(550 == nestedAttrs1.get("N2").intValue());
            Map<String, ? extends Number> nestedAttrs2 = clusterData.getNestedMapAttributes().get("nested2");
            
            assertTrue(500 == nestedAttrs2.get("N3").intValue());
            assertTrue(550 == nestedAttrs2.get("N4").intValue());
            
            // now remove all hosts, that too using multiple threads.
            threadPool = Executors.newFixedThreadPool(numThreads);
            for (final TestWorker worker : workers) {
                threadPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        clusterData.removeDataForHost(worker.host);
                        return null;
                    }
                });
            }
            
            threadPool.awaitTermination(1, TimeUnit.SECONDS);

            clusterData.performPostProcessing();
            
            System.out.println(clusterData.getNumericAttributes());
            System.out.println(clusterData.getStringAttributes());
            System.out.println(clusterData.getNestedMapAttributes());

            for(Long value : clusterData.getNumericAttributes().values()) {
                assertTrue(value == 0L);
            }
            for(String value : clusterData.getStringAttributes().values()) {
                assertTrue(value.length() == 0);
            }
            
            nestedAttrs1 = clusterData.getNestedMapAttributes().get("nested1");
            assertTrue(0 == nestedAttrs1.get("N1").intValue());
            assertTrue(0 == nestedAttrs1.get("N2").intValue());
            nestedAttrs2 = clusterData.getNestedMapAttributes().get("nested2");
            assertTrue(0 == nestedAttrs2.get("N3").intValue());
            assertTrue(0 == nestedAttrs2.get("N4").intValue());
        }

        private String getFormattedString(Map<String, Long> strMap) throws JsonGenerationException, JsonMappingException, IOException {
            ObjectWriter writer = new ObjectMapper().prettyPrintingWriter(new MinimalPrettyPrinter());
            return writer.writeValueAsString(strMap);
        }
    }
}
