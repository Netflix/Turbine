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

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.Sets;
import com.netflix.eureka2.client.EurekaClient;
import com.netflix.eureka2.interests.ChangeNotification;
import com.netflix.eureka2.interests.Interest;
import com.netflix.eureka2.interests.Interests;
import com.netflix.eureka2.registry.DataCenterInfo;
import com.netflix.eureka2.registry.InstanceInfo;
import com.netflix.eureka2.registry.NetworkAddress;
import com.netflix.eureka2.registry.ServicePort;
import com.netflix.eureka2.registry.datacenter.BasicDataCenterInfo;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import rx.Observable;
import rx.observers.TestSubscriber;

public class EurekaInstanceDiscoveryTest {

    private InstanceInfo upInstanceInfo1;
    private InstanceInfo downInstanceInfo1;
    private InstanceInfo upInstanceInfo2;

    private EurekaClient testEurekaClient;
    private EurekaInstanceDiscovery eurekaInstanceDiscovery;

    @Before
    public void setUp() {
        NetworkAddress defaultAddress = NetworkAddress.NetworkAddressBuilder.aNetworkAddress()
                .withLabel("public")
                .withHostName("hostname")
                .withProtocolType(NetworkAddress.ProtocolType.IPv4)
                .build();

        DataCenterInfo defaultDataCenterInfo = new BasicDataCenterInfo.Builder()
                .withName("default")
                .withAddresses(defaultAddress)
                .build();

        this.upInstanceInfo1 = new InstanceInfo.Builder()
                .withId("id-1")
                .withApp("API")
                .withStatus(InstanceInfo.Status.UP)
                .withDataCenterInfo(defaultDataCenterInfo)
                .withPorts(Sets.newHashSet(new ServicePort(8080, false)))
                .build();

        this.downInstanceInfo1 = new InstanceInfo.Builder()
                .withId("id-1")
                .withApp("API")
                .withStatus(InstanceInfo.Status.DOWN)
                .withDataCenterInfo(defaultDataCenterInfo)
                .withPorts(Sets.newHashSet(new ServicePort(8080, false)))
                .build();

        this.upInstanceInfo2 = new InstanceInfo.Builder()
                .withId("id-2")
                .withApp("API")
                .withStatus(InstanceInfo.Status.UP)
                .withDataCenterInfo(defaultDataCenterInfo)
                .withPorts(Sets.newHashSet(new ServicePort(8080, false)))
                .build();

        testEurekaClient = new TestEurekaClient();
        eurekaInstanceDiscovery = new EurekaInstanceDiscovery(testEurekaClient);
    }

    @After
    public void tearDown() {
        testEurekaClient.close();
    }

    @Test
    public void testUpdateToSameInstance() {
        EurekaInstance a = EurekaInstance.from(new ChangeNotification<>(ChangeNotification.Kind.Add, upInstanceInfo1));
        EurekaInstance b = EurekaInstance.from(new ChangeNotification<>(ChangeNotification.Kind.Add, downInstanceInfo1));

        Assert.assertEquals(EurekaInstance.Status.UP, a.getStatus());
        Assert.assertEquals(EurekaInstance.Status.DOWN, b.getStatus());

        TestSubscriber<EurekaInstance> ts = new TestSubscriber<>();

        testEurekaClient.register(upInstanceInfo1);
        testEurekaClient.update(downInstanceInfo1);

        eurekaInstanceDiscovery.getInstanceEvents("API").subscribe(ts);

        ts.assertReceivedOnNext(Arrays.asList(a, b));
    }

    @Test
    public void testInstanceRegisterUnregister() {
        EurekaInstance a = EurekaInstance.from(new ChangeNotification<>(ChangeNotification.Kind.Add, upInstanceInfo1));
        EurekaInstance b = EurekaInstance.from(new ChangeNotification<>(ChangeNotification.Kind.Delete, upInstanceInfo1));

        Assert.assertEquals(EurekaInstance.Status.UP, a.getStatus());
        Assert.assertEquals(EurekaInstance.Status.DOWN, b.getStatus());

        TestSubscriber<EurekaInstance> ts = new TestSubscriber<>();

        testEurekaClient.register(upInstanceInfo1);
        testEurekaClient.unregister(upInstanceInfo1);

        eurekaInstanceDiscovery.getInstanceEvents("API").subscribe(ts);

        ts.assertReceivedOnNext(Arrays.asList(a, b));
    }

    @Test
    public void testMultipleInstances() {
        EurekaInstance a = EurekaInstance.from(new ChangeNotification<>(ChangeNotification.Kind.Add, upInstanceInfo1));
        EurekaInstance b = EurekaInstance.from(new ChangeNotification<>(ChangeNotification.Kind.Delete, upInstanceInfo1));
        EurekaInstance c = EurekaInstance.from(new ChangeNotification<>(ChangeNotification.Kind.Add, upInstanceInfo2));

        Assert.assertEquals(EurekaInstance.Status.UP, a.getStatus());
        Assert.assertEquals(EurekaInstance.Status.DOWN, b.getStatus());
        Assert.assertEquals(EurekaInstance.Status.UP, c.getStatus());

        TestSubscriber<EurekaInstance> ts = new TestSubscriber<>();

        testEurekaClient.register(upInstanceInfo1);
        testEurekaClient.unregister(upInstanceInfo1);
        testEurekaClient.register(upInstanceInfo2);

        eurekaInstanceDiscovery.getInstanceEvents("API").subscribe(ts);

        ts.assertReceivedOnNext(Arrays.asList(a, b, c));
    }


    private static class TestEurekaClient extends EurekaClient {
        private Queue<ChangeNotification<InstanceInfo>> messages;

        TestEurekaClient() {
            this.messages = new ConcurrentLinkedQueue<>();
        }

        @Override
        public Observable<Void> register(InstanceInfo instanceInfo) {
            messages.add(new ChangeNotification<>(ChangeNotification.Kind.Add, instanceInfo));
            return Observable.empty();
        }

        @Override
        public Observable<Void> update(InstanceInfo instanceInfo) {
            messages.add(new ChangeNotification<>(ChangeNotification.Kind.Modify, instanceInfo));
            return Observable.empty();
        }

        @Override
        public Observable<Void> unregister(InstanceInfo instanceInfo) {
            messages.add(new ChangeNotification<>(ChangeNotification.Kind.Delete, instanceInfo));
            return Observable.empty();
        }

        @Override
        public Observable<ChangeNotification<InstanceInfo>> forInterest(Interest<InstanceInfo> interest) {
            return Observable.from(messages);
        }

        @Override
        public Observable<ChangeNotification<InstanceInfo>> forApplication(String s) {
            return forInterest(Interests.forApplications(s));
        }

        @Override
        public Observable<ChangeNotification<InstanceInfo>> forVips(String... strings) {
            throw new RuntimeException("Not Implemented");
        }

        @Override
        public void close() {
            messages.clear();
        }
    }
}
