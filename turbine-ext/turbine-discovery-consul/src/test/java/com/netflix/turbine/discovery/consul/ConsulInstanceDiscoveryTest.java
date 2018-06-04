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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import rx.Observable;
import rx.observers.TestSubscriber;

import com.netflix.turbine.discovery.consul.ConsulInstance;
import com.netflix.turbine.discovery.consul.ConsulInstanceDiscovery;

public class ConsulInstanceDiscoveryTest {

    @Test
    public void testDeltaRemoveDuplicateAddSecond() {
        ConsulInstance a = new ConsulInstance("", ConsulInstance.Status.UP, "hostname1", 1234);
        Observable<List<ConsulInstance>> first = Observable.just(a).toList();

        ConsulInstance b = new ConsulInstance("", ConsulInstance.Status.DOWN, "hostname1", 1234);
        Observable<List<ConsulInstance>> second = Observable.just(a, b).toList();

        TestSubscriber<ConsulInstance> ts = new TestSubscriber<ConsulInstance>();

        Observable
                .just(first, second)
                .flatMap(o -> o)
                .startWith(new ArrayList<ConsulInstance>())
                .buffer(2, 1)
                .filter(l -> l.size() == 2)
                .flatMap(ConsulInstanceDiscovery::delta)
                .subscribe(ts);

        ts.assertReceivedOnNext(Arrays.asList(a, b));
    }

    @Test
    public void testDrop() {
        ConsulInstance a = new ConsulInstance("", ConsulInstance.Status.UP, "hostname1", 1234);
        Observable<List<ConsulInstance>> first = Observable.just(a).toList();

        ConsulInstance b = new ConsulInstance("", ConsulInstance.Status.DOWN, "hostname1", 1234);
        Observable<List<ConsulInstance>> second = Observable.just(b).toList();

        TestSubscriber<ConsulInstance> ts = new TestSubscriber<ConsulInstance>();

        Observable
                .just(first, second)
                .flatMap(o -> o)
                .startWith(new ArrayList<ConsulInstance>())
                .buffer(2, 1)
                .filter(l -> l.size() == 2)
                .flatMap(ConsulInstanceDiscovery::delta)
                .subscribe(ts);

        ts.assertReceivedOnNext(Arrays.asList(a, b));
    }

    @Test
    public void testAddRemoveAddRemove() {
        // start with 4
        ConsulInstance a1 = new ConsulInstance("", ConsulInstance.Status.UP, "hostname1", 1234);
        ConsulInstance a2 = new ConsulInstance("", ConsulInstance.Status.UP, "hostname2", 1234);
        ConsulInstance a3 = new ConsulInstance("", ConsulInstance.Status.UP, "hostname3", 1234);
        ConsulInstance a4 = new ConsulInstance("", ConsulInstance.Status.UP, "hostname4", 1234);
        Observable<List<ConsulInstance>> first = Observable.just(a1, a2, a3, a4).toList();

        // mark one of them as DOWN
        ConsulInstance b4 = new ConsulInstance("", ConsulInstance.Status.DOWN, "hostname4", 1234);
        Observable<List<ConsulInstance>> second = Observable.just(a1, a2, a3, b4).toList();

        // then completely drop 2 of them
        Observable<List<ConsulInstance>> third = Observable.just(a1, a2).toList();

        TestSubscriber<ConsulInstance> ts = new TestSubscriber<ConsulInstance>();

        Observable
                .just(first, second, third)
                .flatMap(o -> o)
                .startWith(new ArrayList<ConsulInstance>())
                .buffer(2, 1)
                .filter(l -> l.size() == 2)
                .flatMap(ConsulInstanceDiscovery::delta)
                .subscribe(ts);

        // expected ...
        // UP a1, UP a2, UP a3, UP a4
        // DOWN b4
        // DOWN a3
        ts.assertReceivedOnNext(Arrays.asList(a1, a2, a3, a4, b4, new ConsulInstance(a3.getCluster(), ConsulInstance.Status.DOWN, a3.getHost(), a3.getPort()), b4));
    }
}
