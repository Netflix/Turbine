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

import java.net.URI;

import com.netflix.turbine.discovery.StreamAction;
import com.netflix.turbine.discovery.StreamDiscovery;
import com.orbitz.consul.Consul;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class ConsulStreamDiscovery implements StreamDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(ConsulStreamDiscovery.class);

    public static ConsulStreamDiscovery create(String appName, String uriTemplate, Consul consul) {
        return new ConsulStreamDiscovery(appName, uriTemplate, consul);
    }

    public final static String HOSTNAME = "{HOSTNAME}";
    private final String uriTemplate;
    private final String appName;
    private final Consul consul;

    private ConsulStreamDiscovery(String appName, String uriTemplate, Consul consul) {
        this.appName = appName;
        this.uriTemplate = uriTemplate;
        this.consul = consul;
    }

    @Override
    public Observable<StreamAction> getInstanceList() {
        return new ConsulInstanceDiscovery(consul)
                .getInstanceEvents(appName)
                .map(ei -> {
                    URI uri;
                    String uriString = uriTemplate.replace(HOSTNAME, ei.getHost() + ":" + ei.getPort());
                    try {
                        uri = new URI(uriString);
                    } catch (Exception e) {
                        throw new RuntimeException("Invalid URI: " + uriString, e);
                    }
                    if (ei.getStatus() == ConsulInstance.Status.UP) {
                        logger.info("StreamAction ADD");
                        return StreamAction.create(StreamAction.ActionType.ADD, uri);
                    } else {
                        logger.info("StreamAction REMOVE");
                        return StreamAction.create(StreamAction.ActionType.REMOVE, uri);
                    }

                });
    }

}
