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

import com.netflix.eureka2.client.Eureka;
import com.netflix.eureka2.client.EurekaClient;
import com.netflix.eureka2.client.resolver.ServerResolvers;
import com.netflix.eureka2.interests.ChangeNotification;
import com.netflix.eureka2.registry.InstanceInfo;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

/**
 * Class that encapsulates an {@link InstanceDicovery} implementation that uses Eureka (see https://github.com/Netflix/eureka)
 * The plugin requires a list of applications configured. It then queries the set of instances for each application.
 * Instance information retrieved from Eureka must be translated to something that Turbine can understand i.e the
 * {@link EurekaInstance} class.
 * 
 * All the logic to perform this translation can be overridden here, so that you can provide your own implementation if needed.
 */
public class EurekaInstanceDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(EurekaInstanceDiscovery.class);

    private final EurekaClient eurekaClient;

    public static void main(String[] args) {
        OptionParser optionParser = new OptionParser();
        optionParser.accepts("eurekaPort").withRequiredArg();
        optionParser.accepts("eurekaHostname").withRequiredArg();
        optionParser.accepts("app").withRequiredArg();

        OptionSet options = optionParser.parse(args);

        int eurekaPort = -1;
        if (!options.has("eurekaPort")) {
            System.err.println("Argument --eurekaPort required: port of eurekaServer");
            System.exit(-1);
        } else {
            try {
                eurekaPort = Integer.parseInt(String.valueOf(options.valueOf("eurekaPort")));
            } catch (NumberFormatException e) {
                System.err.println("Value of eurekaPort must be an integer but was: " + options.valueOf("eurekaPort"));
            }
        }

        String eurekaHostname = null;
        if (!options.has("eurekaHostname")) {
            System.err.println("Argument --eurekaHostname required: hostname of eurekaServer");
            System.exit(-1);
        } else {
            eurekaHostname = String.valueOf(options.valueOf("eurekaHostname"));
        }

        String app = null;
        if (!options.has("app")) {
            System.err.println("Argument --app required for Eureka instance discovery. Eg. -app api");
            System.exit(-1);
        } else {
            app = String.valueOf(options.valueOf("app"));
        }

        EurekaClient eurekaClient = Eureka.newClient(ServerResolvers.just(eurekaHostname, eurekaPort), null);
        new EurekaInstanceDiscovery(eurekaClient)
                .getInstanceEvents(app).toBlocking().forEach(i -> System.out.println(i));
    }

    public EurekaInstanceDiscovery(EurekaClient eurekaClient) {
        this.eurekaClient = eurekaClient;
    }

    public Observable<EurekaInstance> getInstanceEvents(String appName) {
        return eurekaClient.forApplication(appName)
                .map(new Func1<ChangeNotification<InstanceInfo>, EurekaInstance>() {
                    @Override
                    public EurekaInstance call(ChangeNotification<InstanceInfo> notification) {
                        try {
                            return EurekaInstance.from(notification);
                        } catch (Exception e) {
                            logger.warn("Error parsing notification from eurekaClient {}", notification);
                        }
                        return null;
                    }
                }).filter(new Func1<EurekaInstance, Boolean>() {
                    @Override
                    public Boolean call(EurekaInstance eurekaInstance) {
                        return eurekaInstance != null;
                    }
                });
    }
}