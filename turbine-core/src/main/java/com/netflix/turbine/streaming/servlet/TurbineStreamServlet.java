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
package com.netflix.turbine.streaming.servlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.turbine.data.AggDataFromCluster;
import com.netflix.turbine.monitor.cluster.ClusterMonitor;
import com.netflix.turbine.monitor.cluster.ClusterMonitorFactory;
import com.netflix.turbine.plugins.PluginsFactory;
import com.netflix.turbine.streaming.RelevanceConfig;
import com.netflix.turbine.streaming.StreamingDataHandler;
import com.netflix.turbine.streaming.TurbineStreamingConnection;

/**
 * Stream responses to browser according to Server-Side Event spec.
 * 
 * <p>The servlet first attempts to find the {@link ClusterMonitor} for the specified <b>cluster</b> param. 
 * Once this is found it creates the {@link TurbineStreamingConnection} and then gives it it's own {@link StreamingDataHandler} to handle events from the streaming connection. 
 * All data written to the StreamingHandler is then sent out over the http connection using the Server-Side Event spec.
 * 
 */
public class TurbineStreamServlet extends HttpServlet {
    
    private static final long serialVersionUID = 1L;
    private final static Logger logger = LoggerFactory.getLogger(TurbineStreamServlet.class);

    /**
     * @param request {@link HttpServletRequest}
     * @param response {@link HttpServletResponse}
     * @throws ServletException, IOException
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
    
    /**
     * @param request {@link HttpServletRequest}
     * @param response {@link HttpServletResponse}
     * @throws ServletException, IOException
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        
        int delay = -1;
        String delayParam = request.getParameter("delay");
        if (delayParam != null) {
            delay = Integer.parseInt(delayParam);
        }
        
        Collection<FilterCriteria> criteria = FilterCriteria.getFilterCriteria(request);
        logger.info("FilterCriteria: " + criteria);
        Set<String> statsTypesFilter = getFilteredStatsTypes(request);
        logger.info("StatsType filters: " + statsTypesFilter);
        
        try {
            String clusterName = request.getParameter("cluster");
            if (clusterName == null) {
                // Use the default cluster if none specified.
                // This would work with the default property such as:
                // turbine.ConfigPropertyBasedDiscovery.default.instances=[ comma separated list ]
                clusterName = "default";
            }
            
            response = new SynchronizedHttpServletResponse(response);
            
            response.setHeader("Content-Type", "text/event-stream;charset=UTF-8");
            response.setHeader("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
            response.setHeader("Pragma", "no-cache");

            streamFromCluster(response, clusterName, criteria, delay);
            
        } catch (Exception e) {
            logger.error("We failed to start the streaming connection", e);
        }
    }
    
    /**
     * Private helper that streams data from the specified cluster. 
     * @param response      : the response to write the data back to
     * @param clusterName   : the specified cluster name
     * @param criteria      : the filter criteria for filtering / sorting data
     * @param delay         : delay stream by millis
     * @throws Exception
     */
    private static void streamFromCluster(HttpServletResponse response,
                                          String clusterName, 
                                          Collection<FilterCriteria> criteria, 
                                          int delay) 
        throws Exception {
        
        ClusterMonitorFactory<AggDataFromCluster> clusterMonFactory 
            = (ClusterMonitorFactory<AggDataFromCluster>) PluginsFactory.getClusterMonitorFactory();
        if (clusterMonFactory == null) {
            throw new RuntimeException("Must configure plugin for ClusterMonitorFactory");
        }
        
        ClusterMonitor<AggDataFromCluster> clusterMonitor = clusterMonFactory.getClusterMonitor(clusterName);
        if (clusterMonitor == null) {
            response.sendError(404, "Cluster not found");
            return;
        }
        
        TurbineStreamingConnection<AggDataFromCluster> connection = 
                new TurbineStreamingConnection<AggDataFromCluster>(new ServletStreamHandler(response), criteria, delay);
        try {
            clusterMonitor.registerListenertoClusterMonitor(connection);
            clusterMonitor.startMonitor();
            connection.waitOnConnection(); // start the streaming event handler
            logger.info("\n\n\n\nRETURNING FROM waitOnConnection: " + connection.getName());
            
        } catch(Exception e) {
            logger.info("Caught ex. Stopping StreamingConnection", e);
        } catch(Throwable  t) {
            logger.info("Caught throwable. StreamingConnection", t);
        } finally {
            if (connection != null) {
                clusterMonitor.getDispatcher().deregisterEventHandler(connection);
            }
        }
    }

    private Set<String> getFilteredStatsTypes(HttpServletRequest request) {
       
        Set<String> typeFilters = new HashSet<String>();
        
        String statsTypeFilter = getServletConfig().getInitParameter("statsTypeFilter");
        if(statsTypeFilter != null) {
            String[] statsTypes = statsTypeFilter.split(",");
            if(statsTypes != null && statsTypes.length > 0) {
                for(String statsType : statsTypes) {
                    typeFilters.add(statsType);
                }
            }
        }
        if (request.getParameter("type") != null) {
            String typeString = request.getParameter("type").trim();
            if (typeString.length() > 0) {
                String types[] = typeString.split(",");
                for (String t : types) {
                    String tValue = t.trim().toUpperCase();
                    typeFilters.add(tValue);
                }
            }
        }

        return typeFilters;
    }

    private static class ServletStreamHandler implements StreamingDataHandler {

        private int responseFlushDelay = 100; // don't flush more often than this
        private volatile long lastResponseFlush = -1;

        private static final String DATA_PREFIX = "data: ";
        private static final String DOUBLE_NEWLINE = "\n\n";
        private static final String PING_STRING = ": ping\n";

        private final HttpServletResponse response;
        private ServletStreamHandler(HttpServletResponse resp) {
            response = resp;
        }
        @Override
        public void writeData(String data) throws Exception {
            
            long currentTime = System.currentTimeMillis();
            
              // output the data
              response.getWriter().print(DATA_PREFIX + data + DOUBLE_NEWLINE);

              // make sure we flush and don't have the message stuck in a buffer
              // but we don't want to do it too often otherwise we overload the socket and blow-up with Broken Pipes
              if (lastResponseFlush == -1 || currentTime > lastResponseFlush + responseFlushDelay) {
                  response.flushBuffer();
                  lastResponseFlush = currentTime;
              }
        }

        @Override
        public void deleteData(String type, Set<String> names) throws Exception {

            String prefix = "data: {\"deleteData\":\"true\", \"type\":\"" + type + "\", \"name\":\""; 
            StringBuilder sb = new StringBuilder();
            for (String s : names) {
                sb.append(prefix).append(s).append("\"}\n\n");
            }

            String deleteData = sb.toString();

            synchronized(response) {
                response.getWriter().print(deleteData);
                response.flushBuffer();
            }
        }

        @Override
        public void noData() throws Exception {
            response.getWriter().print(PING_STRING);
            response.flushBuffer();
        }
    }
    
    /**
     * Helper class that understands how to parse the filter criteria passed in using the query params
     * 
     *  e.g   
     *  filterCriteria=type:foo|name:bar,type:baz|name:floz
     * 
     * This will filter 2 sub streams of type bar and baz and will look for metrics with the name foo and floz
     *
     */
    public static class FilterCriteria {
        
        public String name; 
        public String type; 
        public String prefix; 
        
        public RelevanceConfig relevanceConfig; 
        
        public static FilterCriteria parseCriteria(String s) {

            // defaults for relevance config
            int topN = -1;
            Map<String, Double> sortCriteria = new HashMap<String, Double>();

            FilterCriteria filter = new FilterCriteria();

            String[] parts = s.split("\\|");
            for (String part : parts) {

                String[] kvPair = part.split(":");
                if (kvPair.length != 2) {
                    throw new RuntimeException("Malformed filter criteria config, missing ':' in " + s);
                }
                String key = kvPair[0];
                String value = kvPair[1];

                if(key.equals("name")) {
                    filter.name = value;
                } else if (key.equals("type")) {
                    filter.type = value;
                } else if (key.equals("prefix")) {
                    filter.prefix = value;
                } else if (key.equals("topN")) {
                    topN = Integer.parseInt(value);
                } else {
                    // this is sort criteria
                    sortCriteria.put(key, Double.parseDouble(value));
                }
            }

            if (filter.type != null && topN > 0 && sortCriteria.size() > 0) {
                filter.relevanceConfig = new RelevanceConfig("name", filter.type, topN, sortCriteria);
            }
            return filter;
        }

        public static Collection<FilterCriteria> getFilterCriteria(HttpServletRequest request) {
            
            List<FilterCriteria> criteria = new ArrayList<FilterCriteria>();
            
            String filterCriteriaString = request.getParameter("filterCriteria");
            if (filterCriteriaString != null) {
                String[] parts = filterCriteriaString.split(",");
                if (parts != null && parts.length > 0) {
                    for(String part : parts) {
                        criteria.add(parseCriteria(part));
                    }
                }
            }
            return criteria;
        }
    }
}
