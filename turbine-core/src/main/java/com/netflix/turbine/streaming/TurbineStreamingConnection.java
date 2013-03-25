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
package com.netflix.turbine.streaming;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.util.MinimalPrettyPrinter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.turbine.data.DataFromSingleInstance;
import com.netflix.turbine.data.TurbineData;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.handler.PerformanceCriteria;
import com.netflix.turbine.handler.TurbineDataHandler;
import com.netflix.turbine.monitor.cluster.ClusterMonitor;
import com.netflix.turbine.streaming.RelevanceKey.RelevanceComparator;
import com.netflix.turbine.streaming.servlet.TurbineStreamServlet.FilterCriteria;

/**
 * Class that represents an output stream from a {@link ClusterMonitor}
 * <p>TurbineStreamingConnection provides the following functionality
 * <ul>
 * <li>Streaming filtered data that matches a given data <b>type</b>
 * <li>Streaming filtered data that matches a given data <b>name</b>
 * <li>Streaming filtered data that starts with a given <b>name</b> prefix
 * <li>Sorting data according to the specified {@link RelevanceConfig}</b>
 * <li>Buffering data for a specified string <b>delay</b>
 * <li>De-duping updates on data that is not changing often
 * <li>Sending ping heartbeats to signal inactivity to downstream {@link StreamingDataHandler} listeners
 * </ul>
 *
 * @param <T>
 */
public class TurbineStreamingConnection<T extends TurbineData> implements TurbineDataHandler<T> {

    private final static Logger logger = LoggerFactory.getLogger(TurbineStreamingConnection.class);

    private static final Object CurrentTime = "currentTime";

    protected final String name; 
    protected final StreamingDataHandler streamHandler; 
    private volatile boolean stopMonitoring = false;
    private volatile long lastEvent = -1;
    /* an artificial delay for how fast we stream to the client ... raw speed is too fast for mobile devices and takes huge CPU even on powerful desktops */
    protected int streamingDelay = 100;
    private final PerformanceCriteria perfCriteria;

    protected final Set<String> filterPrefixes;
    private final Set<String> statsTypeFilter;
    private final Set<String> dataNames;
    
    private final Map<String, RelevantMetrics> relevantMetrics; 

    /**
     * Signal controls used to co-ordinate the data handler thread and the ping thread. 
     * In case the relevance criteria is specified, we need to periodically sort the data using the ping thread. 
     * But the ping thread runs once every 3 seconds and hence the sorted results (relevant data) can take a while to surface. 
     * Furthermore, we should sort only when we have enough relevant data, and this comes from another thread - the data handler thread(s)
     * Also, the data handler thread should try as much as possible to not send non-relevant data out to the wire, but what is relevant is decided by the other 'ping' thread. 
     * For balancing a fast real-time experience with good relevant (sorted topN) data, we need to co-ordinate these 2 independent threads without any expensive synchronization primitives. 
     * 
     * Hence I'm using the following mechanism based on a state machine model to achieve this. 
     * The different states are DoNotSort --> StartSorting --> Sorted and occur in the order specified. The state transitions occur exactly in this direction and there 
     * is no other transition. 
     * 
     * 1. DoNotSort    - is the initial state that we start out in where the sorting thread ( calling waitOnConnection() ) is not allowed to sort, 
     *                   since there isn't enough data yet. The data thread is still gathering enough data and determining whether it is worthy of being sorted.
     *                   While in this state the sorting thread spins for a max of 3 seconds for each relevant metric type, and the data thread does not spit 
     *                   out any data to downstream listeners. 
     * 2. StartSorting - once the data thread gets enough data, it signals the sorting thread to start doing it's work. Again during this state, the data thread
     *                   does not spit any data out. 
     * 3. Sorted       - This is the last state and is signalled by the sorting thread, which then unblocks the data thread who can then stream data out as long 
     *                   as the data is within the topN list.                   
     */
    
    /* allow 'state' to be remembered during the lifetime of this streaming connection ... ConcurrentHashMap since 'handleEvent' can theoretically occur from multiple threads even though it likely won't be. */
    protected ConcurrentHashMap<String, Object> streamingConnectionSession = new ConcurrentHashMap<String, Object>();

    /*
     * used to track if a JMXData object has new data or not so we can skip sending it to the browser if it hasn't changed
     * <p>
     * NOTE: We specifically do this here and not the AbstractJMXMonitor because we want the cache done PER CONNECTION so that each connection
     * will get each message at least once when they first connect and then only get new messages if they change, thus we need the "state" of each connection
     * which AbstractJMXMonitor does not have.
     * <p>
     * This makes a huge difference (at least on small clusters) in the amount of data being delivered and quite significant bandwidth/cpu/performance benefits to the client browser.
     * It then allows us to reduce the latency between updates so that things that are changing get updated faster without incurring overhead of sending large
     * amounts of data for things that haven't changed.
     */
    private final ConcurrentHashMap<String, String> dataHash = new ConcurrentHashMap<String, String>();
    private final ConcurrentHashMap<String, AtomicLong> lastOutputForTypeCache = new ConcurrentHashMap<String, AtomicLong>();

    private final ObjectWriter objectWriter;
    
    @SuppressWarnings("deprecation")
    public TurbineStreamingConnection(StreamingDataHandler sHandler, Collection<FilterCriteria> criteria, int delay) throws Exception {

        this.streamHandler = sHandler;
        this.dataNames = getNameFilters(criteria);
        this.statsTypeFilter = getTypeFilters(criteria);
        this.filterPrefixes = getFilterPrefixes(criteria);
        
        this.name = "StreamingHandler_" + UUID.randomUUID().toString();

        this.streamingDelay = delay;

        this.perfCriteria = new BroweserPerfCriteria();
        
        // setup the relevant metrics, if any specified
        relevantMetrics = new HashMap<String, RelevantMetrics>();
        
        Set<RelevanceConfig> relevanceConfig = getRelevanceConfig(criteria);
        logger.info("Relevance config: " + relevanceConfig);
        for (RelevanceConfig rConfig : relevanceConfig) {
            relevantMetrics.put(rConfig.type, new RelevantMetrics(rConfig));
        }
        logger.info("Relevance metrics config: " + relevantMetrics);
        
        ObjectMapper objectMapper = new ObjectMapper();
        
        objectWriter = objectMapper.prettyPrintingWriter(new MinimalPrettyPrinter());
    }

    @Override
    public String getName() {
        return this.name;
    }
    
    /**
     * Static config for the perf criteria for the browser. 
     * We really don't need to over provision for this, since the browser based event handler does not have to do much.
     * It simply listens to data from the agg cluster monitor (event handler) above and then writes it out to the 
     * specific n/w stream. 
     * 
     */
    private class BroweserPerfCriteria implements PerformanceCriteria {
        
        @Override
        public boolean isCritical() {
            return false;
        }

        @Override
        public int getMaxQueueSize() {
            return 1000;
        }

        @Override
        public int numThreads() {
            return 1;
        }
    }
    
    /**
     * This will continue streaming responses and will block until the client connection breaks or the cluster monitor above fails to give data
     */
    public void waitOnConnection() {

        try {
            /* hold the connection open */
            while (!stopMonitoring) {
                
                // a safety-net that will break us out of here if handleData is not getting called.
                if (lastEvent > -1 && System.currentTimeMillis() - lastEvent > 10000) {
                    // it's been a while since we received an update so let's kill this.
                    logger.info("ERROR: We haven't heard from the monitor in a while so are killing the handler and will stop streaming to client.");
                    stopMonitoring = true;
                }

                try {
                    // try writing to the stream to determine if the connection is still alive, if not close things
                    streamHandler.noData();
                    
                    long start = System.currentTimeMillis();
                    
                    for (RelevantMetrics metrics : relevantMetrics.values()) {

                        // Keep spinning till we timeout after 3 seconds, or till we are told to start sorting by the data thread
                        while ((metrics.state.get() == State.DoNotSort) && 
                                ((System.currentTimeMillis() - start) < 3000)) {
                            Thread.sleep(10);
                        }

                        // we either timed out waiting, in which case State will still be in DoNotSort mode,
                        // or we were explicitly told to StartSorting
                        if (metrics.state.get() != State.DoNotSort) {
                            
                            boolean deleteData = metrics.sort();
                            
                            // in case this is the first time, push out the sorted data ... why wait for the data thread
                            if (metrics.state.get() == State.StartSorting) {
                                for (String key : metrics.topN.get()) {
                                    TurbineData json = metrics.dataMap.get(key);
                                    streamHandler.writeData(objectWriter.writeValueAsString(json));
                                }
                                metrics.state.set(State.Sorted);
                            }

                            // Push out the delete data if any
                            if (deleteData) {
                                Set<String> deleteSet = metrics.deletedSet.get();
                                if (deleteSet != null && deleteSet.size() > 0) {
                                    streamHandler.deleteData(metrics.config.type, deleteSet);
                                }
                            }
                        }
                    }

                } catch (Exception e) {
                    if ("Broken pipe".equals(e.getMessage())) { 
                        // use debug instead of error as this is expected to happen every time a client disconnects
                        logger.debug("Broken pipe (most likely client disconnected) when writing to response stream", e);
                    } else {
                        logger.error("Got exception when writing to response stream", e);
                    }
                    stopMonitoring = true;
                }

                // spend most time asleep, the handleData method via another thread will do all the work
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    stopMonitoring = true;
                    logger.info("Got interrupted when waiting on connection", e);
                }
            }
        } catch(Throwable t) {
            logger.error("Caught throwable when waiting on connection", t);
        } finally {
            // we have lost the client connection or some other event has told us to stop that broke us out of the loop above so we want to stop receiving events
        }
    }

    
    @Override
    public void handleData(Collection<T> data) {
        
        if (stopMonitoring) {
            // we have been stopped so don't try handling data
            return;
        }
        writeToStream(data);
    }

    @Override
    public void handleHostLost(Instance host) {
        // we don't need to do anything here
    }
    

    protected void writeToStream(Collection<? extends TurbineData> dataCollection) {
        
        // record that we received an event
        lastEvent = System.currentTimeMillis();
        try {

            for (TurbineData data : dataCollection) {
                
                try {
                    // drop the data on the floor if it is not in the StatsType filter
                    if(statsTypeFilter.size() != 0) {
                        if(!statsTypeFilter.contains(data.getType())) {
                            continue;
                        }
                    }

                    // filter on metric name (if any specified)
                    if(dataNames.size() != 0) {
                        if(!dataNames.contains(data.getName())) {
                            continue;
                        }
                    }

                    // drop the data on the floor if we are filtering out specific metrics
                    if (filterPrefixes.size() > 0) {
                        boolean allowData = false;
                        for (String f : filterPrefixes) {
                            if (data.getName().contains(f)) {
                                allowData = true;
                                break;
                            }
                        }
                        // we didn't find it so skip this one
                        if (!allowData) {
                            continue;
                        }
                    }

                    String dataKey = getDataKey(data);
                    if (!isDataWithinStreamingWindow(dataKey)) {
                        continue;
                    }

                    /* prevent processing/sending of duplicate data objects when data upstream isn't changing */
                    Map<String, Object> attrs = data.getAttributes();
                    attrs.remove(CurrentTime);
                    
                    HashMap<String, Map<String, ? extends Number>> nestedAttrs = data.getNestedMapAttributes(); 
                    if (nestedAttrs != null && nestedAttrs.keySet().size() > 0) {
                        for (String nestedMapKey : nestedAttrs.keySet()) {
                            Map<String, ? extends Number> nestedMap = nestedAttrs.get(nestedMapKey);
                            if (nestedMap != null) {
                                attrs.put(nestedMapKey, nestedMap);
                            }
                        }
                    }
                    
                    String jsonStringForDataHash = objectWriter.writeValueAsString(attrs);
                    String lastMessageForDataType = dataHash.get(dataKey);
                    if (lastMessageForDataType != null && lastMessageForDataType.equals(jsonStringForDataHash)) {
                        logger.debug("We skipped delivering a message since it hadn't changed: " + dataKey);
                        continue;
                    } else {
                        // store the raw json string so we skip it next time if this data object doesn't change
                        dataHash.put(dataKey, jsonStringForDataHash);
                    }

                    // allow post-processing of the data specific to the needs of streaming and the monitoring UI
                        
                    // check if the data is in the relevant metrics, if any relevance criteria was specified
                    RelevantMetrics metrics = relevantMetrics.get(data.getType());
                    
                    if(metrics != null) {
                        // add metrics to the sort obj
                        String key = data.getName();
                        metrics.put(key, data);
                        
                        // If the sort obj is still in the initial state, then check if we can start it
                        if (metrics.state.get() == State.DoNotSort) {
                            if (metrics.dataMap.size() >= metrics.config.topN) {
                                metrics.state.compareAndSet(State.DoNotSort, State.StartSorting); // ok to lose, means someone else started sorting, which is ok
                            }
                        }
                        
                        // if we have a sorted set, then it is ok to push data to downstream listeners
                        if (metrics.state.get() == State.Sorted) {
                            if (metrics.isInTopN(data.getName())) {
                                streamHandler.writeData(jsonStringForDataHash);
                            }
                        }
                    } else {
                        // there is no TopN sort configured for the stream, push the data downstream
                        streamHandler.writeData(jsonStringForDataHash);
                    }

                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    if (data == null) {
                        logger.error("Failed to process data but will continue processing in loop.", e);
                    } else {
                        logger.error("Failed to process data with type:" + data.getType() + " but will continue processing in loop.", e);
                    }
                }

            } // end of for loop for all stats data

        } catch (IOException e) {
            // this means the client closed the connection
            logger.debug("We lost the client connection. Will stop monitoring and streaming.", e);
            stopMonitoring = true;
        } catch (Throwable t) {
            logger.error("An unknown error occurred trying to write data to client stream. Will stop monitoring and streaming.", t);
            stopMonitoring = true;
        }
    }

    private Set<String> getNameFilters(Collection<FilterCriteria> criteria) {
        Set<String> set = new HashSet<String>();
        for(FilterCriteria filter : criteria) {
            if(filter.name != null) {
                set.add(filter.name);
            }
        }
        return set;
    }

    private Set<String> getTypeFilters(Collection<FilterCriteria> criteria) {
        Set<String> set = new HashSet<String>();
        for(FilterCriteria filter : criteria) {
            if(filter.type != null) {
                set.add(filter.type);
            }
            // add in the types from the relevance config as well, if any specified.
            if(filter.relevanceConfig != null && filter.relevanceConfig.type != null) {
                set.add(filter.relevanceConfig.type);
            }
        }
        return set;
    }

    private Set<String> getFilterPrefixes(Collection<FilterCriteria> criteria) {
        Set<String> set = new HashSet<String>();
        for(FilterCriteria filter : criteria) {
            if(filter.prefix != null) {
                set.add(filter.prefix);
            }
        }
        return set;
    }    
    
    private Set<RelevanceConfig> getRelevanceConfig(Collection<FilterCriteria> criteria) {
        Set<RelevanceConfig> set = new HashSet<RelevanceConfig>();
        for(FilterCriteria filter : criteria) {
            if(filter.relevanceConfig != null) {
                set.add(filter.relevanceConfig);
            }
        }
        return set;
    }


    private String getDataKey(TurbineData data) {
        String dataKey = data.getClass().getSimpleName() + "_" + data.getKey();
        if (data instanceof DataFromSingleInstance) {
            dataKey += "_" + ((DataFromSingleInstance) data).getHost().getHostname();
        }
        return dataKey;
    }

    private boolean isDataWithinStreamingWindow(String dataKey) {
        long currentTime = System.currentTimeMillis();

        // most of the time we'll get a value from this call
        AtomicLong lastOutputForType = lastOutputForTypeCache.get(dataKey);
        long currentLastOutputForType = -1;
        if (lastOutputForType == null) {
            // the first time we see a dataKey we come in here (and have a thread-race)
            AtomicLong previous = lastOutputForTypeCache.putIfAbsent(dataKey, new AtomicLong(currentTime));
            if (previous != null) {
                // another thread raced us and won
                // which also means we should not proceed with
                // this thread since it just processed the 'dataKey' we also are trying to process
                return false;
            }
            return true;
        } else {
            // get and remember the current value
            currentLastOutputForType = lastOutputForType.get();
            // we got the output so check if we've waited past 'streamingDelay' before we push anything more to the client
            if (streamingDelay != -1 && (currentTime < (currentLastOutputForType + streamingDelay))) {
                return false;
            } else {
                if (!lastOutputForType.compareAndSet(currentLastOutputForType, currentTime)) {
                    // this means another thread raced us and won, which also means we should not proceed with
                    // this thread since it just processed the 'dataKey' we also are trying to process
                    return false;
                }
                return true;
            }
        }
    }
    
    private enum State {
        DoNotSort, StartSorting, Sorted; 
    }
    
    class RelevantMetrics { 
        
        private final RelevanceConfig config;
        private AtomicReference<Set<String>> topN;
        private AtomicReference<Set<String>> deletedSet;
        private final AtomicReference<State> state = new AtomicReference<State>(State.DoNotSort);
        
        private ConcurrentHashMap<String, TurbineData> dataMap = new ConcurrentHashMap<String, TurbineData>();
        
        public RelevantMetrics(RelevanceConfig c) {

            config = c;
            if(config != null) {
                logger.info("Relevance metrics are enabled: " + config.toString());
                topN = new AtomicReference<Set<String>>(null);
                deletedSet = new AtomicReference<Set<String>>(null);
            }
        }
        
        public void put(String key, TurbineData data) {
            dataMap.put(key, data);
        }
        
        public boolean sort() {
            // init the sorted set
            ConcurrentSkipListSet<RelevanceKey> set = new ConcurrentSkipListSet<RelevanceKey>(new RelevanceComparator());
            for (String s : dataMap.keySet()) {
                RelevanceKey key = new RelevanceKey(s, config.items, dataMap.get(s).getNumericAttributes());
                set.add(key);
            }
            
            Set<String> newSet = new HashSet<String>();
            Iterator<RelevanceKey> iter = set.descendingIterator();
            int n = 0;
            while(iter.hasNext() && n < config.topN) {
                newSet.add(iter.next().getName());
                n++;
            }
            
            // override the new set that should allow metrics to start getting filtered with the top list
            Set<String> oldSet = topN.get();

            
            topN.set(newSet);
            deletedSet.set(null);
            
            if (oldSet == null) {
                // this is probably the first sort. So use the old set as any of the stuff that could have gotten out there.
                oldSet = dataMap.keySet();
            }

            // we need to compute the set of elements that need to be discarded from the previous topN list, if any
            Set<String> previousSet = new HashSet<String>(oldSet);
            previousSet.removeAll(newSet);
            
            if (previousSet.size() > 0) {
                deletedSet.set(previousSet);
                return true;
            }
            return false;
        }
        
        public boolean isInTopN(String key) {
            Set<String> topNSet = topN.get();
            if (topNSet == null || topNSet.size() == 0) {
                return true;
            }
            return topNSet.contains(key);
        }
        
        @Override
        public String toString() {
            return config.toString();
        }
    }

    @Override
    public PerformanceCriteria getCriteria() {
        return perfCriteria;
    }
    
    public static class UnitTest {
        
        @Test
        public void testFilterStream() throws Exception {
            
            final AtomicInteger dataCount = new AtomicInteger(0);
            final AtomicInteger pingCount = new AtomicInteger(0);
            final AtomicInteger deleteCount = new AtomicInteger(0);
            final AtomicReference<String> text = new AtomicReference<String>(null);
            
            final HashMap<String, Object> testAttrs = new HashMap<String, Object>();
            final Instance host = new Instance("host", "cluster", true);

            final AtomicBoolean stop = new AtomicBoolean(false);

            final StreamingDataHandler handler = new StreamingDataHandler() {

                @Override
                public void writeData(String data) throws Exception {
                    System.out.println("Data: " + data);
                    if (stop.get()) {
                        throw new RuntimeException("stop!");
                    }
                    dataCount.incrementAndGet();
                    text.set(data);
                }

                @Override
                public void deleteData(String type, Set<String> names) throws Exception {
                    if (stop.get()) {
                        throw new RuntimeException("stop!");
                    }
                    deleteCount.incrementAndGet();
                }

                @Override
                public void noData() throws Exception {
                    if (stop.get()) {
                        throw new RuntimeException("stop!");
                    }
                    pingCount.incrementAndGet();
                }
            };
            
            List<FilterCriteria> filterCriteria = new ArrayList<FilterCriteria>();
            filterCriteria.add(FilterCriteria.parseCriteria("type:testType|name:testName"));

            final TurbineStreamingConnection<TurbineData> connection = 
                    new TurbineStreamingConnection<TurbineData>(handler, filterCriteria, 10);
            
            ExecutorService threadPool = Executors.newFixedThreadPool(10);
            List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
            
             for (int i=0; i<3; i++) {
                 futures.add(threadPool.submit(new Callable<Integer>() {

                    final AtomicInteger count = new AtomicInteger(0);
                    final Random random = new Random();
                    @Override
                    public Integer call() throws Exception {
                        while (!stop.get()) {
                            Collection<TurbineData> data = getRandomData();
                            connection.handleData(data);
                            count.addAndGet(data.size());
                            Thread.sleep(50);
                        }
                        return count.get();
                    }

                    private Collection<TurbineData> getRandomData() {
                        int size = random.nextInt(10);
                        
                        List<TurbineData> list = new ArrayList<TurbineData>();
                        for (int i=0; i<size; i++) {
                            list.add(new DataFromSingleInstance(null, "testType", "testName", host, testAttrs, 0L));
                            list.add(new DataFromSingleInstance(null, "foo", "bar", host, testAttrs, 0L));
                        }
                        return list;
                    }
                    
                }));
            }
             
             final Timer timer = new Timer();
             timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    stop.set(true);
                    timer.cancel();
                }
             }, 3000);
             
             connection.waitOnConnection();
             threadPool.shutdownNow();

             int sum = 0;
             for (Future<Integer> f : futures) {
                 sum += f.get();
             }
             
             assertTrue(sum > dataCount.get());
             assertTrue(1 == dataCount.get());
             assertTrue(0 == deleteCount.get());
             assertTrue(pingCount.get() >= 1);
             assertTrue(text.get(), text.get().contains("testType"));
             assertTrue(text.get().contains("testName"));
             assertFalse(text.get().contains("foo"));
             assertFalse(text.get().contains("bar"));
        }
    }
}
