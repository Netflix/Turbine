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
package com.netflix.turbine.monitor.instance;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.turbine.data.DataFromSingleInstance;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.handler.PerformanceCriteria;
import com.netflix.turbine.handler.TurbineDataDispatcher;
import com.netflix.turbine.handler.TurbineDataHandler;
import com.netflix.turbine.monitor.MonitorConsole;
import com.netflix.turbine.monitor.TurbineDataMonitor;

/**
 * Class that represents a single connection to an {@link Instance}.
 * <p>The InstanceMonitor connects to the host as prescribed by the {@link InstanceUrlClosure} and then streams data down.
 * It assumes that the data format is json.
 * <p>The monitor looks for 1st class attributes <b>name</b> and <b>type</b> and the rest of the attributes are parsed into one of 2 attribute maps
 * i.e either numericAttributes or stringAttributes.
 *
 * <p>The monitor also keeps retrying the host connection unless told to do otherwise. It gives up reconnecting when it finds a bad status code
 * such as a 404 not found or {@link UnknownHostException}.
 * <p>However the monitor continues to retry the connection even when it receives an {@link IOException} in order to be resilient against network flakiness.
 *
 * <p>Each InstanceMonitor has access to the {@link TurbineDataDispatcher} to dispatch data to. This helps it be decoupled from the {@link TurbineDataHandler} underneath.
 * If the dispatcher tells the monitor that there is no one listening then the monitor shuts itself down for efficiency.
 */
public class InstanceMonitor extends TurbineDataMonitor<DataFromSingleInstance> {

    private static final Logger logger = LoggerFactory.getLogger(InstanceMonitor.class);

    private static final ThreadFactory InstanceMonitorThreadFactory = new ThreadFactory() {
        private static final String ThreadName = "InstanceMonitor";

        private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

        public Thread newThread(Runnable r) {
            Thread thread = defaultFactory.newThread(r);
            thread.setName(ThreadName);
            return thread;
        }
    };

    public static final ExecutorService ThreadPool = Executors.newCachedThreadPool(InstanceMonitorThreadFactory);

    // whether we should employ our "skip line" logic to avoid latency buildups
    private static DynamicBooleanProperty skipLineLogic = DynamicPropertyFactory.getInstance().getBooleanProperty("turbine.InstanceMonitor.eventStream.skipLineLogic.enabled", true);
    // how latent responses need to be before we trigger the skip logic
    private static DynamicIntProperty latencyThreshold = DynamicPropertyFactory.getInstance().getIntProperty("turbine.InstanceMonitor.eventStream.skipLineLogic.latencyThreshold", 2500);
    // how long we should wait before processing lines again after a 'latencyThreshold' is met
    private static DynamicIntProperty skipLogicDelay = DynamicPropertyFactory.getInstance().getIntProperty("turbine.InstanceMonitor.eventStream.skipLineLogic.delay", 500);
    private static DynamicIntProperty hostRetryMillis = DynamicPropertyFactory.getInstance().getIntProperty("turbine.InstanceMonitor.hostRertyMillis", 1000);

    // Tracking state for InstanceMonitor
    private enum State {
        NotStarted, Running, StopRequested, CleanedUp;
    }

    private final AtomicReference<State> monitorState = new AtomicReference<State>(State.NotStarted);

    // the StatsInstance host to connect to
    private final Instance host;
    private final TurbineDataDispatcher<DataFromSingleInstance> dispatcher;
    private final MonitorConsole<DataFromSingleInstance> monitorConsole;

    private BufferedReader reader ;

    private final GatewayHttpClient gatewayHttpClient;

    private final String url;

    // some constants
    private static final String NAME_KEY = "name";
    private static final String TYPE_KEY = "type";
    private static final String CURRENT_TIME = "currentTime";

    private static final String DATA_PREFIX = "data";
    private static final String OPEN_BRACE = "{";
    private static final String REPORTING_HOSTS = "reportingHosts";

    private final ObjectReader objectReader;
    private volatile Future<Void> taskFuture;
    private final AtomicLong lastEventUpdateTime = new AtomicLong(-1L);

    // useful for debugging purposes, remove later
    private final DynamicBooleanProperty LogEnabled;

    /**
     * @param host - the host to monitor
     * @param urlClosure - config on how to connect to host
     * @param dispatcher - the dispatcher to send data to
     * @param monitorConsole - the console to manage itself in on startup and shutdown
     */
    public InstanceMonitor(Instance host,
                           InstanceUrlClosure urlClosure,
                           TurbineDataDispatcher<DataFromSingleInstance> dispatcher,
                           MonitorConsole<DataFromSingleInstance> monitorConsole) {
        this(host, new ProdGatewayHttpClient(), urlClosure, dispatcher, monitorConsole);
    }

    private InstanceMonitor(Instance host,
                            GatewayHttpClient httpClient,
                            InstanceUrlClosure urlClosure,
                            TurbineDataDispatcher<DataFromSingleInstance> dispatcher,
                            MonitorConsole<DataFromSingleInstance> monitorConsole) {
        this.host = host;
        this.gatewayHttpClient = httpClient;
        this.dispatcher = dispatcher;
        this.monitorConsole = monitorConsole;
        this.url = urlClosure.getUrlPath(host);
        logger.info("Url for host: " + url + " " + host.getCluster());

        ObjectMapper objectMapper = new ObjectMapper();
        objectReader = objectMapper.reader(Map.class);

        LogEnabled = DynamicPropertyFactory.getInstance().getBooleanProperty("InstanceMonitor.LogEnabled." + host.getHostname(), false);
    }

    /**
     * The name of the InstanceMonitor.
     * @return String
     */
    @Override
    public String getName() {
        return host.getHostname();
    }

    /**
     * @return {@link Instance}
     */
    @Override
    public Instance getStatsInstance() {
        return host;
    }

    /**
     * @return {@link TurbineDataDispatcher}<{@link DataFromSingleInstance}>
     */
    @Override
    public TurbineDataDispatcher<DataFromSingleInstance> getDispatcher() {
        return dispatcher;
    }

    /**
     * Start monitoring
     */
    @Override
    public void startMonitor() throws Exception {
        // This is the only state that we allow startMonitor to proceed in
        if (monitorState.get() != State.NotStarted) {
            return;
        }

        taskFuture = ThreadPool.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {

                try {
                    init();
                    monitorState.set(State.Running);
                    while(monitorState.get() == State.Running) {
                        doWork();
                    }
                } catch(Throwable t) {
                    logger.warn("Stopping InstanceMonitor for: " + getStatsInstance().getHostname() + " " + getStatsInstance().getCluster(), t);
                } finally {
                    if (monitorState.get() == State.Running) {
                        monitorState.set(State.StopRequested);
                    }
                    cleanup();
                    monitorState.set(State.CleanedUp);
                }
                return null;
            }
        });
    }

    /**
     * Request monitor to stop
     */
    public void stopMonitor() {
        monitorState.set(State.StopRequested);
        logger.info("Host monitor stop requested: " + getName());

        if (taskFuture != null) {
            logger.info("Cancelling InstanceMonitor task future");
            taskFuture.cancel(true);
        }
    }

    @Override
    public long getLastEventUpdateTime() {
        return lastEventUpdateTime.get();
    }

    /**
     * Private helper which does all the work
     * @throws Exception
     */
    private void doWork() throws Exception {

        DataFromSingleInstance instanceData = null;

        instanceData = getNextStatsData();
        if(instanceData == null) {
            return;
        } else {
            lastEventUpdateTime.set(System.currentTimeMillis());
        }

        List<DataFromSingleInstance> list = new ArrayList<DataFromSingleInstance>();
        list.add(instanceData);

        /* send to all handlers */
        boolean continueRunning = dispatcher.pushData(getStatsInstance(), list);
        if(!continueRunning) {
            logger.info("No more listeners to the host monitor, stopping monitor for: " + host.getHostname() + " " + host.getCluster());
            monitorState.set(State.StopRequested);
            return;
        }
    }

    /**
     * Init resources required
     * @throws Exception
     */
    private void init() throws Exception {

        HttpGet httpget = new HttpGet(url);

        HttpResponse response = gatewayHttpClient.getHttpClient().execute(httpget);

        HttpEntity entity = response.getEntity();
        InputStream is = entity.getContent();
        reader = new BufferedReader(new InputStreamReader(is));

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            // this is unexpected. We probably have the wrong endpoint. Print the response out for debugging and give up here.
            List<String> responseMessage = IOUtils.readLines(reader);
            logger.error("Could not initiate connection to host, giving up: " + responseMessage);
            throw new MisconfiguredHostException(responseMessage.toString());
        }
    }

    /**
     * Cleanup resources created
     * @throws Exception
     */
    private void cleanup() throws Exception {
        if (monitorState.get() == State.CleanedUp) {
            // this has already been done. Return
            return;
        }
        logger.info("Single Server event publisher releasing http client connection for: " + host.getHostname() + " " + host.getCluster());
        gatewayHttpClient.releaseConnections();

        // let the dispatcher know that this monitor is shutting down
        dispatcher.handleHostLost(getStatsInstance());

        // tell the stats console that this monitor is going away.
        logger.info("Removing monitor from StatsEventConsole: " + host.getHostname() + " " + host.getCluster());
        monitorConsole.removeMonitor(getName());

        monitorState.set(State.CleanedUp);
    }

    /**
     * Create {@link DataFromSingleInstance} from every line coming over the network connection
     * @return {@link DataFromSingleInstance}
     * @throws Exception
     */
    private DataFromSingleInstance getNextStatsData() throws Exception {

        long skipProcessingUntil = 0;

        try {
            String line = null;
            while ((line = reader.readLine()) != null) {

                if(Thread.currentThread().isInterrupted()) {
                    logger.info("Current thread is interrupted, returning.");
                    monitorState.set(State.StopRequested);
                    return null;
                }

                long currentTime = System.currentTimeMillis();

                if (skipLineLogic.get()) {
                    if (currentTime < skipProcessingUntil) {
                        // continue ... skip processing and just keep reading from the stream
                        if (logger.isDebugEnabled()) {
                            logger.debug("Skipping event to catch up to end of feed and reduce latency");
                        }
                        continue;
                    }
                }

                line = line.trim();
                if (line.length() == 0 || (!line.startsWith(DATA_PREFIX))) {
                    // empty line or invalid so skip processing to next line
                    continue;
                }

                // we expect JSON on data lines
                int pos = line.indexOf(OPEN_BRACE);
                if (pos < 0) {
                    continue;
                }
                String jsonString = line.substring(pos);

                try {
                    Map<String, Object> json = objectReader.readValue(jsonString);

                    String type = (String) json.remove(TYPE_KEY);
                    String name = (String) json.remove(NAME_KEY);

                    if (type == null || name == null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Type and/or name missing, skipping line: " + line);
                        }
                        return null;
                    }

                    long timeOfEvent = -1;
                    long latency = -1;
                    if (skipLineLogic.get()) {
                            // we have the actual time the event data was created so use that to track latency
                            if (json.containsKey(CURRENT_TIME)) {
                                timeOfEvent = (Long) json.get(CURRENT_TIME);
                            }

                            if (timeOfEvent <= 0) {
                            // default to using now as the time ... it won't track actual latency, but will track from this point on
                            timeOfEvent = System.currentTimeMillis();
                            json.put(CURRENT_TIME, timeOfEvent);
                        }

                        latency = currentTime - timeOfEvent;
                        if (timeOfEvent > 0) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("----> Pushed to cluster. Latency of event: " + latency + " + timeOfEvent: " + timeOfEvent + " [" + name + "]");
                            }
                        }
                    }

                    if (skipLineLogic.get() && timeOfEvent > 0 && latency > latencyThreshold.get()) {
                        // tell the loop to ignore all events for the next X milliseconds so it can just read data from the stream and catch up
                        if (logger.isDebugEnabled()) {
                            logger.info("Telling stream reader to wait for 1 second before processing anymore so it can catch up." + latency);
                        }
                        skipProcessingUntil = System.currentTimeMillis() + skipLogicDelay.get();
                        // we also skip pushing this data since it's latent and we would rather quickly catch up and send current data than sending this latent data

                        markEventDiscarded();
                        continue;
                    }

                    HashMap<String, Long> nAttrs = new HashMap<String, Long>();
                    HashMap<String, String> sAttrs = new HashMap<String, String>();
                    HashMap<String, Map<String, ? extends Number>> mapAttrs = new HashMap<String, Map<String, ? extends Number>>();

                    @SuppressWarnings("unchecked")
                    // the JSONObject doesn't use generics so I'm ignoring the warning
                    Iterator<String> keys = json.keySet().iterator();
                    while (keys.hasNext()) {
                        String key = keys.next();
                        Object value = json.get(key);

                        if (value instanceof Integer) {
                            long longValue = ((Integer) value).longValue();
                            nAttrs.put(key, longValue);
                        } else if (value instanceof Long) {
                            long longValue = (Long) value;
                            nAttrs.put(key, longValue);
                        } else if (value instanceof Map) {
                            Map<String, ? extends Number> mapValue = (Map<String, ? extends Number>) value;
                            mapAttrs.put(key, mapValue);
                        } else {
                            sAttrs.put(key, String.valueOf(value));
                        }
                    }


                    if (!nAttrs.containsKey(REPORTING_HOSTS)) {
                        nAttrs.put(REPORTING_HOSTS, 1L);
                    }

                  return new DataFromSingleInstance(this, type, name, host, nAttrs, sAttrs, mapAttrs, timeOfEvent);

                } catch (JsonParseException e) {
                    if (logger.isDebugEnabled() || LogEnabled.get()) {
                        logger.info("Got JSON Ex for host: " + this.getName() + ", ex:" + e.getMessage() + " " + line, e);
                    }

                    return null;
            } catch (JsonMappingException e) {
                if (logger.isDebugEnabled() || LogEnabled.get()) {
                    logger.info("Got JSON Ex for host: " + this.getName() + ", ex:" + e.getMessage() + " " + line, e);
                }

                return null;
            }
            }

        } catch (IOException ioe) {
            logger.info("Got ioe for host: " + this.getName());
            retryHostConnection();
            return null;
        }
        // if we reach here, then we have no more data from this connection.
        String message = String.format("no more data from connection to %s", this.host.getHostname());
        logger.info(message);
        retryHostConnection();
        return null;
    }

    /**
     * Helper to retry the host connection on network hiccups.
     * @throws Exception
     */
    private void retryHostConnection() throws Exception {

        boolean succeeded = false;

        while(!succeeded && (monitorState.get() == State.Running)) {
            // first clean up connection resources
            this.gatewayHttpClient.releaseConnections();
            try {
                logger.info("Re-initing host connection: " + host.getHostname() + " " + host.getCluster());
                this.init();
                succeeded = true;
            } catch (NoRouteToHostException e) {
                logger.warn("Found no route to host connection: " + host.getHostname() + " " + host.getCluster() + " will not retry", e);
                monitorState.set(State.StopRequested);
            } catch (MisconfiguredHostException e) {
                logger.warn("Found MisconfiguredHostException host connection: " + host.getHostname() + " " + host.getCluster() + " will not retry", e);
                monitorState.set(State.StopRequested);
            } catch (Exception e) {
                logger.warn("Could not init host connection: " + host.getHostname() + " " + host.getCluster() + " will continue to retry", e);
            } finally {
                try {
                    Thread.sleep(hostRetryMillis.get());
                } catch (InterruptedException ie) {
                    logger.warn("Instance Monitor got interrupted");
                    monitorState.set(State.StopRequested);
                }
            }
        }
        if (monitorState.get() != State.Running) {
            throw new Exception("Giving up on retry connection");
        }
    }

    /**
     * @return true/false
     */
    public boolean monitorRunning() {
        return monitorState.get() == State.Running;
    }

    /**
     * @return true/false
     */
    public boolean hasStopped() {
        return monitorState.get() == State.CleanedUp;
    }

    /**
     * Helper interface for unit testing
     */
    private interface GatewayHttpClient {
        HttpClient getHttpClient();
        void releaseConnections();
    }

    public static void stop() {
        ThreadPool.shutdown();
    }

    private static class ProdGatewayHttpClient implements GatewayHttpClient {
        HttpClient httpClient;

        @Override
        public HttpClient getHttpClient() {
            httpClient = new DefaultHttpClient();
            HttpParams httpParams = httpClient.getParams();
            HttpConnectionParams.setConnectionTimeout(httpParams, 10000);
            HttpConnectionParams.setSoTimeout(httpParams, 10000); // we expect a 'ping' at least every 3 seconds even if no data is coming so 4 seconds means something is wrong
            return httpClient;
        }

        @Override
        public void releaseConnections() {
            try {
                if (httpClient != null && httpClient.getConnectionManager() != null) {
                    // When HttpClient instance is no longer needed, shut down the connection manager to ensure immediate deallocation of all system resources
                    httpClient.getConnectionManager().shutdown();
                    httpClient = null;
                }
            } catch (Exception e) {
                logger.error("We failed closing connection to the HTTP server", e);
            }
        }
    }


    private class MisconfiguredHostException extends Exception {

        private static final long serialVersionUID = 1L;

        public MisconfiguredHostException(String arg0) {
            super(arg0);
        }
    }

    public static class UnitTest {

        // all the stuff I could possibly need for unit testing various scenarios.
        Instance instance = new Instance("testInstance", "testCluster", true);
        HttpClient mockClient;
        ClientConnectionManager mockConnManager;
        GatewayHttpClient gatewayClient;

        InstanceMonitor monitor;
        TurbineDataDispatcher<DataFromSingleInstance> dispatcher;

        PerformanceCriteria perfCriteria = new PerformanceCriteria() {
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
                return 0;
            }
        };

        TurbineDataHandler<DataFromSingleInstance> eventHandler;

        File file;

        @Before
        public void before() {
            file = new File("main/testfiles/StatsSingleServerMonitorUnitTest.txt");

            if (!file.exists()) {
                file = new File("testfiles/StatsSingleServerMonitorUnitTest.txt");
            }
        }

        @SuppressWarnings("unchecked")
        private void doTheMockMagic(InputStream dataStream) throws Exception {

            // property override that states that it is ok to receive really old data (we need to do this since we are re-driving old static data through this test)
            ConfigurationManager.getConfigInstance().setProperty("turbine.InstanceMonitor.eventStream.skipLineLogic.enabled", false);

            StatusLine sLine = mock(StatusLine.class);
            when(sLine.getStatusCode()).thenReturn(200);

            HttpResponse mockResponse = mock(HttpResponse.class);
            when(mockResponse.getEntity()).thenReturn(new InputStreamEntity(dataStream, -1));
            when(mockResponse.getStatusLine()).thenReturn(sLine);

            mockConnManager = mock(ClientConnectionManager.class);
            mockClient = mock(HttpClient.class);

            when(mockClient.execute(any(HttpUriRequest.class))).thenReturn(mockResponse);
            when(mockClient.getConnectionManager()).thenReturn(mockConnManager);

            gatewayClient = new GatewayHttpClient() {
                @Override
                public HttpClient getHttpClient() {
                    return mockClient;
                }
                @Override
                public void releaseConnections() {
                    mockConnManager.shutdown();
                }
            };

            InstanceUrlClosure urlClosure = mock(InstanceUrlClosure.class);
            when(urlClosure.getUrlPath(instance)).thenReturn("http://foo.com/");

            dispatcher = new TurbineDataDispatcher<DataFromSingleInstance>("TEST");
            monitor = new InstanceMonitor(instance, gatewayClient, urlClosure, dispatcher, new MonitorConsole<DataFromSingleInstance>());

            eventHandler = mock(TurbineDataHandler.class);
            when(eventHandler.getName()).thenReturn("handler");
            when(eventHandler.getCriteria()).thenReturn(perfCriteria);


            monitor.getDispatcher().registerEventHandler(instance, eventHandler);

            return;
        }

        @Test
        public void testProcessFiniteStream() throws Exception {

            doTheMockMagic(new FileInputStream(file));

            monitor.startMonitor();

            Thread.sleep(1000);

            verify(mockConnManager, times(1)).shutdown();
            verify(eventHandler, never()).handleHostLost(instance);

            monitor.stopMonitor();

            Thread.sleep(500);

            verify(eventHandler, times(1)).handleHostLost(instance);
            assertNull(monitor.getDispatcher().findHandlerForHost(instance, "handler"));

            dispatcher.stopDispatcher();
        }

        class InfiniteInputStream extends InputStream {

            InputStream in = null;
            @Override
            public int read() throws IOException {
                if(in == null) {
                    in = new FileInputStream(file);
                }
                int read = in.read();
                if(read != -1) {
                    return read;
                }
                in.close();
                in = new FileInputStream(file);
                return in.read();
            }
        }


        @Test(timeout=2000)
        public void testProcessInfiniteStream() throws Exception {

            doTheMockMagic(new InfiniteInputStream());

            monitor.startMonitor();
            Thread.sleep(1000);

            assertTrue(monitor.monitorRunning());

            monitor.stopMonitor();

            while(!monitor.hasStopped()) {
                Thread.sleep(50);
            }

            verify(mockConnManager, times(1)).shutdown();
            verify(eventHandler, times(1)).handleHostLost(instance);
            assertNull(monitor.getDispatcher().findHandlerForHost(instance, "handler"));

            dispatcher.stopDispatcher();
        }

        @Test
        public void testInfiniteRetryOnIOException() throws Exception {

            TimeBombInputStream timeBombStream = new TimeBombInputStream();

            doTheMockMagic(timeBombStream);

            monitor.startMonitor();
            Thread.sleep(100);
            assertTrue(monitor.monitorRunning());

            Thread.sleep(3000);

            assertTrue(monitor.monitorRunning());
            assertTrue(timeBombStream.count.get() >= 1);

            verify(mockConnManager, atLeastOnce()).shutdown();
            verify(eventHandler, never()).handleHostLost(instance);

            monitor.stopMonitor();
            assertFalse(monitor.monitorRunning());

            Thread.sleep(2000);

            verify(mockConnManager, atLeastOnce()).shutdown();
            verify(eventHandler, times(1)).handleHostLost(instance);
            assertNull(monitor.getDispatcher().findHandlerForHost(instance, "handler"));

            assertTrue(monitor.hasStopped());

            dispatcher.stopDispatcher();
        }

        @Test(timeout=2000)
        public void testStartMonitorAndNoEventHandlers() throws Exception {

            doTheMockMagic(new InfiniteInputStream());

            monitor.getDispatcher().deregisterEventHandler("handler");
            monitor.startMonitor();
            Thread.sleep(200);

            while(monitor.monitorRunning()) {
                Thread.sleep(50);
            }

            verify(mockConnManager, atLeastOnce()).shutdown();

            assertNull(monitor.getDispatcher().findHandlerForHost(instance, "handler"));

            dispatcher.stopDispatcher();
        }

        private static PerformanceCriteria testCriteria = new PerformanceCriteria() {
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
                return 10;
            }
        };

        private class StatsCounter implements TurbineDataHandler<DataFromSingleInstance> {

            private AtomicInteger count = new AtomicInteger(0);
            private boolean handleHostLostCalled = false;

            String nameS;
            StatsCounter(String name) {
                this.nameS = name;
            }
            @Override
            public String getName() {
                return nameS;
            }

            @Override
            public void handleData(Collection<DataFromSingleInstance> stats) {
                count.addAndGet(stats.size());
            }

            @Override
            public void handleHostLost(Instance host) {
                handleHostLostCalled = true;
            }

            int getCount() {
                return count.get();
            }
            @Override
            public PerformanceCriteria getCriteria() {
                return testCriteria;
            }
        }

        private class TimeBombInputStream extends InfiniteInputStream {

            private volatile boolean blowUp = false;
            private final AtomicInteger count = new AtomicInteger(0);

            private TimeBombInputStream() {
                TimerTask task = new TimerTask() {
                    @Override
                    public void run() {
                        blowUp = true;
                    }
                };
                Timer timer = new Timer();
                timer.schedule(task, 500);
            }

            @Override
            public int read() throws IOException {
                if(blowUp) {
                    count.incrementAndGet();
                    throw new IOException("Kaboom");
                }
                return super.read();
            }
        }

        @Test
        public void testStopMonitorOnNoRouteToHostException() throws Exception {

            TimeBombInputStream timeBombStream = new TimeBombInputStream();

            // property override that states that it is ok to receive really old data (we need to do this since we are re-driving old static data through this test)
            ConfigurationManager.getConfigInstance().setProperty("turbine.InstanceMonitor.eventStream.skipLineLogic.enabled", false);

            StatusLine sLine = mock(StatusLine.class);
            when(sLine.getStatusCode()).thenReturn(200);

            final HttpResponse mockResponse = mock(HttpResponse.class);
            when(mockResponse.getEntity()).thenReturn(new InputStreamEntity(timeBombStream, -1));
            when(mockResponse.getStatusLine()).thenReturn(sLine);

            mockConnManager = mock(ClientConnectionManager.class);
            mockClient = mock(HttpClient.class);

            when(mockClient.execute(any(HttpUriRequest.class))).thenReturn(mockResponse).thenThrow(new NoRouteToHostException());

            when(mockClient.getConnectionManager()).thenReturn(mockConnManager);

            gatewayClient = new GatewayHttpClient() {
                @Override
                public HttpClient getHttpClient() {
                    return mockClient;
                }
                @Override
                public void releaseConnections() {
                    mockConnManager.shutdown();
                }
            };

            InstanceUrlClosure urlClosure = mock(InstanceUrlClosure.class);
            when(urlClosure.getUrlPath(instance)).thenReturn("http://foo.com/");

            dispatcher = new TurbineDataDispatcher<DataFromSingleInstance>("TEST");
            monitor = new InstanceMonitor(instance, gatewayClient, urlClosure, dispatcher, new MonitorConsole<DataFromSingleInstance>());

            eventHandler = mock(TurbineDataHandler.class);
            when(eventHandler.getName()).thenReturn("handler");
            when(eventHandler.getCriteria()).thenReturn(perfCriteria);


            monitor.getDispatcher().registerEventHandler(instance, eventHandler);

            monitor.startMonitor();
            Thread.sleep(2000);
            assertFalse(monitor.monitorRunning());

            verify(mockConnManager, times(2)).shutdown();
            verify(eventHandler, times(1)).handleHostLost(instance);

            assertNull(monitor.getDispatcher().findHandlerForHost(instance, "handler"));
            assertTrue(monitor.hasStopped());

            dispatcher.stopDispatcher();
        }


        @Test
        public void testStartMonitorsAndTransientEventHandlers() throws Exception {

            doTheMockMagic( new InfiniteInputStream());

            monitor.getDispatcher().deregisterEventHandler("handler");

            StatsCounter c1 = new StatsCounter("c1");
            monitor.getDispatcher().registerEventHandler(instance, c1);

            monitor.startMonitor();

            Thread.sleep(500);

            StatsCounter c2 = new StatsCounter("c2");
            monitor.getDispatcher().registerEventHandler(instance, c2);

            Thread.sleep(500);

            assertNotNull(monitor.getDispatcher().findHandlerForHost(instance, "c1"));
            assertNotNull(monitor.getDispatcher().findHandlerForHost(instance, "c2"));
            assertFalse(c1.handleHostLostCalled);
            assertFalse(c2.handleHostLostCalled);

            monitor.getDispatcher().deregisterEventHandler("c2");

            Thread.sleep(500);

            monitor.getDispatcher().deregisterEventHandler("c1");

            while(monitor.monitorRunning()) {
                Thread.sleep(50);
            }

            assertNull(monitor.getDispatcher().findHandlerForHost(instance, "handler"));
            assertNull(monitor.getDispatcher().findHandlerForHost(instance, "c1"));
            assertNull(monitor.getDispatcher().findHandlerForHost(instance, "c2"));

            assertTrue(c1.getCount() > 0);
            assertTrue(c2.getCount() > 0);
            assertTrue(c1.getCount() > c2.getCount());
            assertTrue(c1.handleHostLostCalled);
            assertTrue(c2.handleHostLostCalled);

            dispatcher.stopDispatcher();
        }
    }
}
