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
package com.netflix.turbine;

import java.net.URI;
import java.net.URISyntaxException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class StartTurbine {

    public static void main(String[] args) {
        OptionParser optionParser = new OptionParser();
        optionParser.accepts("port").withRequiredArg();
        optionParser.accepts("streams").withRequiredArg();

        OptionSet options = optionParser.parse(args);
        int port = -1;
        if (!options.has("port")) {
            System.err.println("Argument -port required for SSE HTTP server to start on.");
            System.exit(-1);
        } else {
            try {
                port = Integer.parseInt(String.valueOf(options.valueOf("port")));
            } catch (NumberFormatException e) {
                System.err.println("Value of port must be an integer but was: " + options.valueOf("port"));
            }
        }

        URI[] streams = null;
        if (!options.hasArgument("streams")) {
            System.err.println("Argument -streams required with URIs to connect to. Eg. -streams \"http://host1/metrics.stream http://host2/metrics.stream\"");
            System.exit(-1);
        } else {
            String streamsArg = String.valueOf(options.valueOf("streams"));
            String[] ss = streamsArg.split(" ");
            streams = new URI[ss.length];
            for (int i = 0; i < ss.length; i++) {
                try {
                    streams[i] = new URI(ss[i]);
                } catch (URISyntaxException e) {
                    System.err.println("ERROR: Could not parse stream into URI: " + ss[i]);
                    System.exit(-1);
                }
            }
        }

        if (streams == null || streams.length == 0) {
            System.err.println("There must be at least 1 valid stream URI.");
            System.exit(-1);
        }

        try {
            Turbine.startServerSentEventServer(port, Turbine.aggregateHttpSSE(streams));
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

}
