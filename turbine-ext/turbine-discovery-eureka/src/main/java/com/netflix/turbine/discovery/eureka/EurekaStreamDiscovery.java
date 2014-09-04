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

import com.netflix.turbine.discovery.StreamAction;
import com.netflix.turbine.discovery.StreamDiscovery;

import rx.Observable;

public class EurekaStreamDiscovery implements StreamDiscovery {

    public static EurekaStreamDiscovery create(String appName, String uriTemplate) {
        return new EurekaStreamDiscovery(appName, uriTemplate);
    }

    public final static String HOSTNAME = "{HOSTNAME}";
    private final String uriTemplate;
    private final String appName;

    private EurekaStreamDiscovery(String appName, String uriTemplate) {
        this.appName = appName;
        this.uriTemplate = uriTemplate;
    }

    @Override
    public Observable<StreamAction> getInstanceList() {
        return new EurekaInstanceDiscovery()
                .getInstanceEvents(appName)
                .map(ei -> {
                    URI uri;
                    try {
                        uri = new URI(uriTemplate.replace(HOSTNAME, ei.getHostName()));
                    } catch (Exception e) {
                        throw new RuntimeException("Invalid URI", e);
                    }
                    if (ei.getStatus() == EurekaInstance.Status.UP) {
                        return StreamAction.create(StreamAction.ActionType.ADD, uri);
                    } else {
                        return StreamAction.create(StreamAction.ActionType.REMOVE, uri);
                    }

                });
    }

}
