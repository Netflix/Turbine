/**
 * Copyright 2014 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.turbine.discovery.consul;

import com.orbitz.consul.Consul;
import com.google.common.net.HostAndPort;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.turbine.Turbine;

public class StartConsulTurbine {
    private static final Logger logger = LoggerFactory.getLogger(StartConsulTurbine.class);

    public static void main(String[] args) {
        OptionParser optionParser = new OptionParser();
        optionParser.accepts("port").withRequiredArg();
        optionParser.accepts("app").withRequiredArg();
        optionParser.accepts("urlTemplate").withRequiredArg();
        optionParser.accepts("consulPort").withRequiredArg().defaultsTo("8500");
        optionParser.accepts("consulHostname").withRequiredArg();

        OptionSet options = null;

        try {
            options = optionParser.parse(args);
        } catch (Exception e) {
            System.err.println("Faild to parse input parameters. Expected args: '-port 10901 -app app1 -urlTemplate  http://{HOSTNAME}/stream -consulPort 8500 -consulHostname localhost'");
            System.exit(-1);
        }

        int port = -1;
        if (!options.has("port")) {
            System.err.println("Argument -port required for SSE HTTP server to start on. Eg. -port 8888");
            System.exit(-1);
        } else {
            try {
                port = Integer.parseInt(String.valueOf(options.valueOf("port")));
            } catch (NumberFormatException e) {
                System.err.println("Value of port must be an integer but was: " + options.valueOf("port"));
            }
        }

        String app = null;
        if (!options.has("app")) {
            System.err.println("Argument -app required for Consul Service Discovery. Eg. -app api");
            System.exit(-1);
        } else {
            app = String.valueOf(options.valueOf("app"));
        }

        String template = null;
        if (!options.has("urlTemplate")) {
            System.err.println("Argument -urlTemplate required. Eg. http://" + ConsulStreamDiscovery.HOSTNAME + "/metrics.stream");
            System.exit(-1);
        } else {
            template = String.valueOf(options.valueOf("urlTemplate"));
            if (!template.contains(ConsulStreamDiscovery.HOSTNAME)) {
                System.err.println("Argument -urlTemplate must contain " + ConsulStreamDiscovery.HOSTNAME + " marker. Eg. http://" + ConsulStreamDiscovery.HOSTNAME + "/metrics.stream");
                System.exit(-1);
            }
        }

        int consulPort = -1;
        if (!options.has("consulPort")) {
            System.err.println("Argument --consulPort required: port of Consul Service Discovery");
            System.exit(-1);
        } else {
            try {
                consulPort = Integer.parseInt(String.valueOf(options.valueOf("consulPort")));
            } catch (NumberFormatException e) {
                System.err.println("Value of consulPort must be an integer but was: " + options.valueOf("consulPort"));
            }
        }

        String consulHostname = null;
        if (!options.has("consulHostname")) {
            System.err.println("Argument --consulHostname required: hostname of Consul Service Discovery");
            System.exit(-1);
        } else {
            consulHostname = String.valueOf(options.valueOf("consulHostname"));
        }

        logger.info("Turbine => Consul App: " + app);
        logger.info("Turbine => Consul URL Template: " + template);

        try {
            HostAndPort consulHostAndPort = HostAndPort.fromParts(consulHostname, consulPort);

            Consul consul = Consul.builder()
                            .withHostAndPort(consulHostAndPort)
                            .build();
            
            Turbine.startServerSentEventServer(port, ConsulStreamDiscovery.create(app, template, consul));
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

}
