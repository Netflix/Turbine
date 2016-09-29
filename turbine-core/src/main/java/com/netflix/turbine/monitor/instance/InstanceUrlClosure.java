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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.turbine.discovery.Instance;

import java.util.Map;

/**
 * Class that encapsulates functionality on how to connect to each {@link Instance}
 */
public interface InstanceUrlClosure {
    
    /**
     * Fetch instance url for connecting to it.
     * @param host
     * @return String
     */
    public String getUrlPath(Instance host);
    
    /**
     * Helper class that decides how to connect to a server based on injected config. 
     * Note that the cluster name must be provided here since one can have different configs for different clusters
     */
    public static InstanceUrlClosure ClusterConfigBasedUrlClosure = new InstanceUrlClosure() {

        private final DynamicStringProperty defaultUrlClosureConfig = DynamicPropertyFactory.getInstance().getStringProperty("turbine.instanceUrlSuffix", null);
        @Override
        public String getUrlPath(Instance host) {
            
            if (host.getCluster() == null) {
                throw new RuntimeException("Host must have cluster name in order to use ClusterConfigBasedUrlClosure");
            }
            
            String key = "turbine.instanceUrlSuffix." + host.getCluster();
            DynamicStringProperty urlClosureConfig = DynamicPropertyFactory.getInstance().getStringProperty(key, null);
            
            String url = urlClosureConfig.get();
            if (url == null) {
                url = defaultUrlClosureConfig.get();
            }

            if (url == null) {
                throw new RuntimeException("Config property: " + urlClosureConfig.getName() + " or " +
                        defaultUrlClosureConfig.getName() + " must be set");
            }

            url = processAttributeReplacements(host, url);

            String protocolKey = "turbine.protocol." + host.getCluster();
            DynamicStringProperty protocolConfig = DynamicPropertyFactory.getInstance().getStringProperty(protocolKey, "http");

            return protocolConfig.get() + "://" + host.getHostname() + url;
        }

        /**
         * Replaces any {placeholder} attributes in a url suffix using instance attributes
         *
         * e.x. :{server-port}/hystrix.stream -> :8080/hystrix.stream
         *
         * @param host instance
         * @param url suffix
         * @return replaced url suffix
         */
        private String processAttributeReplacements(Instance host, String url) {
            for (Map.Entry<String, String> attribute : host.getAttributes().entrySet()) {
                String placeholder = "{"+attribute.getKey()+"}";
                if (url.contains(placeholder)) {
                    url = url.replace(placeholder, attribute.getValue());
                }
            }
            return url;
        }
    };
    
    public static class UnitTest {
        
        @Test
        public void testConnectionPath() throws Exception {
            
            Instance host = new Instance("hostname", "testCluster", true);
            ConfigurationManager.getConfigInstance().setProperty("turbine.instanceUrlSuffix.testCluster", ":80/turbine.stream");

            assertEquals("http://hostname:80/turbine.stream", ClusterConfigBasedUrlClosure.getUrlPath(host));

            Instance host2 = new Instance("hostname", "testCluster2", true);
            
            ConfigurationManager.getConfigInstance().setProperty("turbine.instanceUrlSuffix", ":80/global.stream");
            
            assertEquals("http://hostname:80/global.stream", ClusterConfigBasedUrlClosure.getUrlPath(host2));
        }

        @Test
        public void testConnectionPathAttributeReplacement() throws Exception {
            Instance host = new Instance("hostname", "testCluster", true);
            host.getAttributes().put("server-port", "9090");
            ConfigurationManager.getConfigInstance().setProperty("turbine.instanceUrlSuffix.testCluster", ":{server-port}/turbine.stream");

            assertEquals("http://hostname:9090/turbine.stream", ClusterConfigBasedUrlClosure.getUrlPath(host));

            Instance host2 = new Instance("hostname", "testCluster", true);
            host2.getAttributes().put("server-port", "9091");
            host2.getAttributes().put("server-ctx", "hystrix-event");
            ConfigurationManager.getConfigInstance().setProperty("turbine.instanceUrlSuffix.testCluster", ":{server-port}/{server-ctx}/turbine.stream");

            assertEquals("http://hostname:9091/hystrix-event/turbine.stream", ClusterConfigBasedUrlClosure.getUrlPath(host2));
        }
    }
}
