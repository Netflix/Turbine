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
package com.netflix.turbine.discovery.eureka;

import java.net.URI;

import com.netflix.eureka2.client.EurekaClient;
import com.netflix.turbine.discovery.StreamAction;
import com.netflix.turbine.discovery.StreamDiscovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class EurekaStreamDiscovery implements StreamDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(EurekaStreamDiscovery.class);

    public static EurekaStreamDiscovery create(String appName, String uriTemplate, EurekaClient eurekaClient) {
        return new EurekaStreamDiscovery(appName, uriTemplate, eurekaClient);
    }

    public final static String HOSTNAME = "{HOSTNAME}";
    private final String uriTemplate;
    private final String appName;
    private final EurekaClient eurekaClient;

    private EurekaStreamDiscovery(String appName, String uriTemplate, EurekaClient eurekaClient) {
        this.appName = appName;
        this.uriTemplate = uriTemplate;
        this.eurekaClient = eurekaClient;
    }

    @Override
    public Observable<StreamAction> getInstanceList() {
        return new EurekaInstanceDiscovery(eurekaClient)
                .getInstanceEvents(appName)
                .map(ei -> {
                    URI uri;
                    String uriString = uriTemplate.replace(HOSTNAME, ei.getHost() + ":" + ei.getPort());
                    try {
                        uri = new URI(uriString);
                    } catch (Exception e) {
                        throw new RuntimeException("Invalid URI: " + uriString, e);
                    }
                    if (ei.getStatus() == EurekaInstance.Status.UP) {
                        logger.info("StreamAction ADD");
                        return StreamAction.create(StreamAction.ActionType.ADD, uri);
                    } else {
                        logger.info("StreamAction REMOVE");
                        return StreamAction.create(StreamAction.ActionType.REMOVE, uri);
                    }

                });
    }

}
