/*
 * Copyright 2013 Netflix
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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.MyDataCenterInstanceConfig;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.discovery.shared.Application;
import com.netflix.turbine.discovery.eureka.EurekaInstance.Status;

/**
 * Class that encapsulates an {@link InstanceDicovery} implementation that uses Eureka (see https://github.com/Netflix/eureka)
 * The plugin requires a list of applications configured. It then queries the set of instances for each application.
 * Instance information retrieved from Eureka must be translated to something that Turbine can understand i.e the {@link Instance} class.
 * 
 * All the logic to perform this translation can be overriden here, so that you can provide your own implementation if needed.
 */
public class EurekaInstanceDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(EurekaInstanceDiscovery.class);

    public static void main(String[] args) {

        new EurekaInstanceDiscovery().getInstanceEvents("api").toBlocking().forEach(i -> System.out.println(i));
    }

    public EurekaInstanceDiscovery() {
        // initialize eureka client.  make sure eureka properties are properly configured in config.properties
        DiscoveryManager.getInstance().initComponent(new MyDataCenterInstanceConfig(), new DefaultEurekaClientConfig());
    }

    public Observable<EurekaInstance> getInstanceEvents(String appName) {
        return Observable.
                create((Subscriber<? super EurekaInstance> subscriber) -> {
                    try {
                        logger.info("Fetching instance list for app: " + appName);
                        Application app = DiscoveryManager.getInstance().getDiscoveryClient().getApplication(appName);
                        if (app == null) {
                            subscriber.onError(new RuntimeException("App not found: " + appName));
                            return;
                        }
                        List<InstanceInfo> instancesForApp = app.getInstances();
                        if (instancesForApp != null) {
                            logger.info("Received instance list for app: " + appName + " = " + instancesForApp.size());
                            for (InstanceInfo instance : instancesForApp) {
                                if (InstanceInfo.InstanceStatus.UP == instance.getStatus()) {
                                    // we only emit UP instances, the delta process marks DOWN
                                    subscriber.onNext(EurekaInstance.create(instance));
                                }
                            }
                            subscriber.onCompleted();
                        } else {
                            subscriber.onError(new RuntimeException("Failed to retrieve instances for appName: " + appName));
                        }
                    } catch (Throwable e) {
                        subscriber.onError(e);
                    }
                })
                .subscribeOn(Schedulers.io())
                .toList()
                .repeatWhen(a -> a.flatMap(n -> Observable.timer(30, TimeUnit.SECONDS))) // repeat after 30 second delay
                .startWith(new ArrayList<EurekaInstance>())
                .buffer(2, 1)
                .filter(l -> l.size() == 2)
                .flatMap(EurekaInstanceDiscovery::delta);
    }

    static Observable<EurekaInstance> delta(List<List<EurekaInstance>> listOfLists) {
        if (listOfLists.size() == 1) {
            return Observable.from(listOfLists.get(0));
        } else {
            // diff the two
            List<EurekaInstance> newList = listOfLists.get(1);
            List<EurekaInstance> oldList = new ArrayList<>(listOfLists.get(0));

            Set<EurekaInstance> delta = new LinkedHashSet<>();
            delta.addAll(newList);
            // remove all that match in old
            delta.removeAll(oldList);

            // filter oldList to those that aren't in the newList
            oldList.removeAll(newList);

            // for all left in the oldList we'll create DROP events
            for (EurekaInstance old : oldList) {
                delta.add(EurekaInstance.create(Status.DOWN, old.getInstanceInfo()));
            }

            return Observable.from(delta);
        }
    }

}