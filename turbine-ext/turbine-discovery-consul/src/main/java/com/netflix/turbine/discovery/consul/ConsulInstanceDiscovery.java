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
package com.netflix.turbine.discovery.consul;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.collect.Collections2;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.*;
import com.orbitz.consul.cache.*;
import com.orbitz.consul.cache.ConsulCache.*;
import com.orbitz.consul.model.health.*;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Subscriber;

/**
 * Class that encapsulates an {@link InstanceDicovery} implementation that uses
 * Consul (see https://www.consul.io/) The plugin requires a list of
 * applications configured. It then queries the set of instances for each
 * application. Instance information retrieved from Consul must be translated to
 * something that Turbine can understand i.e the {@link ConsulInstance} class.
 * 
 * All the logic to perform this translation can be overridden here, so that you
 * can provide your own implementation if needed.
 */
public class ConsulInstanceDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(ConsulInstanceDiscovery.class);
    static CopyOnWriteArrayList<ConsulInstance> instances=new CopyOnWriteArrayList<>();
    private final Consul consul;
    
    public static void main(String[] args) {
        OptionParser optionParser = new OptionParser();
        optionParser.accepts("consulPort").withRequiredArg();
        optionParser.accepts("consulHostname").withRequiredArg();
        optionParser.accepts("app").withRequiredArg();

        OptionSet options = optionParser.parse(args);

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

        String app = null;
        if (!options.has("app")) {
            System.err.println("Argument --app required for Consul Instsance Discovery. Eg. -app api");
            System.exit(-1);
        } else {
            app = String.valueOf(options.valueOf("app"));
        }

        HostAndPort consulHostAndPort = HostAndPort.fromParts(consulHostname, consulPort);

        Consul consul = Consul.builder().withHostAndPort(consulHostAndPort).build();

        new ConsulInstanceDiscovery(consul).getInstanceEvents(app).toBlocking().forEach(i -> System.out.println(i));
    }

    public ConsulInstanceDiscovery(Consul consul) {
        this.consul = consul;
    }

    public Observable<ConsulInstance> getInstanceEvents(String appName) {
        HealthClient healthClient = consul.healthClient();
        ServiceHealthCache svHealth = ServiceHealthCache.newCache(healthClient, appName);
        ArrayList<Subscriber<ConsulInstance>> subscribers=new ArrayList<>();
        Observable<ConsulInstance> observableConsulInstances = Observable.create((x) ->{
            subscribers.add((Subscriber<ConsulInstance>)x);
            instances.forEach((i) ->{
                x.onNext(i);
            }); 
        });
        
        svHealth.addListener(new Listener<ServiceHealthKey, ServiceHealth>() {
            @Override
            public void notify(Map<ServiceHealthKey, ServiceHealth> newValues) {
                try {
                    newValues.forEach((x, y) -> {
                        Service service = y.getService();
                        Optional<String> dataCenter = y.getNode().getDatacenter();

                        String cluster = dataCenter.isPresent() ? dataCenter.get() : service.getService();
                        ConsulInstance.Status status = ConsulInstance.Status.UP;
                        String hostName = service.getAddress();
                        int port = service.getPort();

                        for (HealthCheck check : y.getChecks()) {
                            if (check.getStatus().equals("passing") == false) {
                                status = ConsulInstance.Status.DOWN;
                            }
                        } 
                        ConsulInstance instance = new ConsulInstance(cluster, status, hostName, port);
                        Collection<ConsulInstance> toRemove = Collections2.filter(instances, (c) ->{return c.getId().equals(instance.getId());});
                        for(ConsulInstance item : toRemove) { instances.remove(item);}
                        instances.add(instance);
                    });
                } catch (Exception e) {
                    logger.warn("Error parsing notification from consul {}", newValues);
                }
            }
        });
        svHealth.start();

        return observableConsulInstances;
    }

    public static Observable<ConsulInstance> delta(List<List<ConsulInstance>> listOfLists) {
        if (listOfLists.size() == 1) {
            return Observable.from(listOfLists.get(0));
        } else {
            // diff the two
            List<ConsulInstance> newList = listOfLists.get(1);
            List<ConsulInstance> oldList = new ArrayList<>(listOfLists.get(0));

            Set<ConsulInstance> delta = new LinkedHashSet<>();
            delta.addAll(newList);
            // remove all that match in old
            delta.removeAll(oldList);

            // filter oldList to those that aren't in the newList
            oldList.removeAll(newList);

            // for all left in the oldList we'll create DROP events
            for (ConsulInstance old : oldList) {
                delta.add(new ConsulInstance(old.getCluster(), ConsulInstance.Status.DOWN, old.getHost(), old.getPort()));
            }

            return Observable.from(delta);
        }
    }
}