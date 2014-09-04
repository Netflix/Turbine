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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.observables.GroupedObservable;

import com.netflix.turbine.aggregator.InstanceKey;
import com.netflix.turbine.aggregator.StreamAggregator;
import com.netflix.turbine.internal.JsonUtility;

public class Demo {

    public static void main(String[] args) {
        getStream().toBlocking().forEach(map -> {
            System.out.println("data => " + JsonUtility.mapToJson(map));
        });

    }

    public static Observable<Map<String, Object>> getStream() {
        GroupedObservable<InstanceKey, Map<String, Object>> hystrixStreamA = HystrixStreamSource.getHystrixStreamFromFile(HystrixStreamSource.STREAM_SUBSCRIBER_CINEMATCH_1, 12345, 500);
        GroupedObservable<InstanceKey, Map<String, Object>> hystrixStreamB = HystrixStreamSource.getHystrixStreamFromFile(HystrixStreamSource.STREAM_SUBSCRIBER_CINEMATCH_1, 23456, 500);
        GroupedObservable<InstanceKey, Map<String, Object>> hystrixStreamC = HystrixStreamSource.getHystrixStreamFromFile(HystrixStreamSource.STREAM_SUBSCRIBER_CINEMATCH_1, 34567, 500);
        GroupedObservable<InstanceKey, Map<String, Object>> hystrixStreamD = HystrixStreamSource.getHystrixStreamFromFile(HystrixStreamSource.STREAM_SUBSCRIBER_CINEMATCH_1, 45678, 500);

        Observable<GroupedObservable<InstanceKey, Map<String, Object>>> fullStream = Observable.just(hystrixStreamA, hystrixStreamB, hystrixStreamC, hystrixStreamD);
        return StreamAggregator.aggregateGroupedStreams(fullStream).flatMap(commandGroup -> {
            return commandGroup
                    .throttleFirst(1000, TimeUnit.MILLISECONDS);
        });
    }
}
