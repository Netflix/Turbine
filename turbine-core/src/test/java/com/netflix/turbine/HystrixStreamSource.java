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


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import com.netflix.turbine.aggregator.InstanceKey;
import com.netflix.turbine.internal.JsonUtility;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;
import rx.observables.GroupedObservable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.TestSubject;

public class HystrixStreamSource {

    public static final String STREAM_ALL = "hystrix";
    public static final String STREAM_SUBSCRIBER = "hystrix-subscriber";
    public static final String STREAM_CINEMATCH = "hystrix-cinematch";
    public static final String STREAM_SUBSCRIBER_CINEMATCH_1 = "hystrix-subscriber_cinematch_1";
    public static final String STREAM_SUBSCRIBER_CINEMATCH_2 = "hystrix-subscriber_cinematch_2";

    public static void main(String[] args) {
        getHystrixStreamFromFile(STREAM_ALL, 12345, 1).take(5).toBlocking().forEach(new Action1<Map<String, Object>>() {

            @Override
            public void call(Map<String, Object> s) {
                System.out.println("s: " + s.keySet());
            }

        });
    }

    // a hack to simulate a stream
    public static GroupedObservable<InstanceKey, Map<String, Object>> getHystrixStreamFromFile(final String stream, final int instanceID, int latencyBetweenEvents) {
        return GroupedObservable.from(InstanceKey.create(instanceID), Observable.create(new OnSubscribe<Map<String, Object>>() {

            @Override
            public void call(Subscriber<? super Map<String, Object>> sub) {
                try {
                    while (!sub.isUnsubscribed()) {
                        String packagePath = HystrixStreamSource.class.getPackage().getName().replace('.', '/');
                        InputStream file = HystrixStreamSource.class.getResourceAsStream("/" + packagePath + "/" + stream + ".stream");
                        BufferedReader in = new BufferedReader(new InputStreamReader(file));
                        String line = null;
                        while ((line = in.readLine()) != null && !sub.isUnsubscribed()) {
                            if (!line.trim().equals("")) {
                                if (line.startsWith("data: ")) {
                                    String json = line.substring(6);
                                    try {
                                        Map<String, Object> jsonMap = JsonUtility.jsonToMap(json);
                                        jsonMap.put("instanceId", String.valueOf(instanceID));
                                        sub.onNext(jsonMap);
                                        Thread.sleep(latencyBetweenEvents);
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    sub.onError(e);
                }
            }

        }).subscribeOn(Schedulers.newThread()));
    }

    public static GroupedObservable<InstanceKey, Map<String, Object>> getHystrixStreamFromFileEachLineScheduledEvery10Milliseconds(final String stream, final int instanceID, final TestScheduler scheduler, int maxTime) {
        TestSubject<Map<String, Object>> scheduledOrigin = TestSubject.create(scheduler);
        try {
            String packagePath = HystrixStreamSource.class.getPackage().getName().replace('.', '/');
            InputStream file = HystrixStreamSource.class.getResourceAsStream("/" + packagePath + "/" + stream + ".stream");
            BufferedReader in = new BufferedReader(new InputStreamReader(file));
            String line = null;
            int time = 0;
            while ((line = in.readLine()) != null && time < maxTime) {
                if (!line.trim().equals("")) {
                    if (line.startsWith("data: ")) {
                        time = time + 10; // increment by 10 milliseconds
                        String json = line.substring(6);
                        try {
                            Map<String, Object> jsonMap = JsonUtility.jsonToMap(json);
                            //                            System.err.println(instanceID + " => scheduling at time: " + time + " => " + jsonMap);
                            scheduledOrigin.onNext(jsonMap, time);
                        } catch (Exception e) {
                            System.err.println("bad data");
                        }
                    }
                }
            }
            scheduledOrigin.onCompleted(maxTime);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return GroupedObservable.from(InstanceKey.create(instanceID), scheduledOrigin.subscribeOn(scheduler));
    }
}
