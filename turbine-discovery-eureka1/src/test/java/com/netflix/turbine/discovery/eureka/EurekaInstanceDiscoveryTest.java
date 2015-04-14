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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import rx.Observable;
import rx.observers.TestSubscriber;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.turbine.discovery.eureka.EurekaInstance;
import com.netflix.turbine.discovery.eureka.EurekaInstanceDiscovery;
import com.netflix.turbine.discovery.eureka.EurekaInstance.Status;

public class EurekaInstanceDiscoveryTest {

    @Test
    public void testDeltaRemoveDuplicateAddSecond() {
        EurekaInstance a = EurekaInstance.create(InstanceInfo.Builder.newBuilder()
                .setAppName("api").setHostName("hostname1").setStatus(InstanceStatus.UP).build());
        Observable<List<EurekaInstance>> first = Observable.just(a).toList();

        EurekaInstance b = EurekaInstance.create(InstanceInfo.Builder.newBuilder()
                .setAppName("api").setHostName("hostname1").setStatus(InstanceStatus.DOWN).build());
        Observable<List<EurekaInstance>> second = Observable.just(a, b).toList();

        TestSubscriber<EurekaInstance> ts = new TestSubscriber<EurekaInstance>();

        Observable
                .just(first, second)
                .flatMap(o -> o)
                .startWith(new ArrayList<EurekaInstance>())
                .buffer(2, 1)
                .filter(l -> l.size() == 2)
                .flatMap(EurekaInstanceDiscovery::delta)
                .subscribe(ts);

        ts.assertReceivedOnNext(Arrays.asList(a, b));
    }

    @Test
    public void testDrop() {
        EurekaInstance a = EurekaInstance.create(InstanceInfo.Builder.newBuilder()
                .setAppName("api").setHostName("hostname1").setStatus(InstanceStatus.UP).build());
        Observable<List<EurekaInstance>> first = Observable.just(a).toList();

        EurekaInstance b = EurekaInstance.create(InstanceInfo.Builder.newBuilder()
                .setAppName("api").setHostName("hostname1").setStatus(InstanceStatus.DOWN).build());
        Observable<List<EurekaInstance>> second = Observable.just(b).toList();

        TestSubscriber<EurekaInstance> ts = new TestSubscriber<EurekaInstance>();

        Observable
                .just(first, second)
                .flatMap(o -> o)
                .startWith(new ArrayList<EurekaInstance>())
                .buffer(2, 1)
                .filter(l -> l.size() == 2)
                .flatMap(EurekaInstanceDiscovery::delta)
                .subscribe(ts);

        ts.assertReceivedOnNext(Arrays.asList(a, b));
    }

    @Test
    public void testAddRemoveAddRemove() {
        // start with 4
        EurekaInstance a1 = EurekaInstance.create(InstanceInfo.Builder.newBuilder()
                .setAppName("api").setHostName("hostname1").setStatus(InstanceStatus.UP).build());
        EurekaInstance a2 = EurekaInstance.create(InstanceInfo.Builder.newBuilder()
                .setAppName("api").setHostName("hostname2").setStatus(InstanceStatus.UP).build());
        EurekaInstance a3 = EurekaInstance.create(InstanceInfo.Builder.newBuilder()
                .setAppName("api").setHostName("hostname3").setStatus(InstanceStatus.UP).build());
        EurekaInstance a4 = EurekaInstance.create(InstanceInfo.Builder.newBuilder()
                .setAppName("api").setHostName("hostname4").setStatus(InstanceStatus.UP).build());
        Observable<List<EurekaInstance>> first = Observable.just(a1, a2, a3, a4).toList();

        // mark one of them as DOWN
        EurekaInstance b4 = EurekaInstance.create(InstanceInfo.Builder.newBuilder()
                .setAppName("api").setHostName("hostname4").setStatus(InstanceStatus.DOWN).build());
        Observable<List<EurekaInstance>> second = Observable.just(a1, a2, a3, b4).toList();

        // then completely drop 2 of them
        Observable<List<EurekaInstance>> third = Observable.just(a1, a2).toList();

        TestSubscriber<EurekaInstance> ts = new TestSubscriber<EurekaInstance>();

        Observable
                .just(first, second, third)
                .flatMap(o -> o)
                .startWith(new ArrayList<EurekaInstance>())
                .buffer(2, 1)
                .filter(l -> l.size() == 2)
                .flatMap(EurekaInstanceDiscovery::delta)
                .subscribe(ts);

        // expected ...
        // UP a1, UP a2, UP a3, UP a4
        // DOWN b4
        // DOWN a3
        ts.assertReceivedOnNext(Arrays.asList(a1, a2, a3, a4, b4, EurekaInstance.create(Status.DOWN, a3.getInstanceInfo()), b4));
    }
}
