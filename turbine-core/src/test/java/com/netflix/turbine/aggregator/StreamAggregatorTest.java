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
package com.netflix.turbine.aggregator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Test;

import rx.Observable;
import rx.observables.GroupedObservable;
import rx.observers.TestSubscriber;
import rx.schedulers.TestScheduler;
import rx.subjects.TestSubject;

import com.netflix.turbine.HystrixStreamSource;

public class StreamAggregatorTest {
    /**
     * Submit 3 events containing `rollingCountSuccess` of => 327, 370, 358
     * 
     * We should receive a GroupedObservable of key "CinematchGetPredictions" with deltas => 327, 43, -12, -358 (onComplete)
     */
    @Test
    public void testNumberValue_OneInstanceOneGroup() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<Object> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).flatMap(commandGroup -> {
            System.out.println("======> Got group for command: " + commandGroup.getKey());
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                return data.get("rollingCountSuccess");
            });
        }).subscribe(ts);

        stream.onNext(getCinematchCommandInstanceStream(12345, scheduler), 5);
        stream.onCompleted(100);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        ts.assertNoErrors();
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect a single instance
        assertEquals(1, numGroups.get());
        // the expected deltas for rollingCountSuccess
        ts.assertReceivedOnNext(Arrays.asList(327L, 370L, 358L, 0L));
    }

    /**
     * Group 1: 327, 370, 358 => deltas: 327, 43, -12, -358 (onComplete)
     * Group 2: 617, 614, 585 => deltas: 617, -3, -29, -585 (onComplete)
     * 
     * 
     */
    @Test
    public void testNumberValue_OneInstanceTwoGroups() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<Object> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).flatMap(commandGroup -> {
            System.out.println("======> Got group for command: " + commandGroup.getKey());
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                return data.get("rollingCountSuccess");
            });
        }).subscribe(ts);

        stream.onNext(getSubscriberAndCinematchCommandInstanceStream(12345, scheduler), 0);
        stream.onCompleted(100);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect 2 commands
        assertEquals(2, numGroups.get());
        // the expected deltas for rollingCountSuccess (2 instances of same data grouped together)
        ts.assertReceivedOnNext(Arrays.asList(327L, 617L, 370L, 614L, 358L, 585L, 0L, 0L)); // two 0s because both groups complete and remove themselves
    }

    /**
     * Two instances emitting: 327, 370, 358 => deltas: 327, 43, -12, -358 (onComplete)
     * 
     * 327, 327, 370, 370, 358, 358
     * 
     * 0 + 327 = 327
     * 327 + 327 = 654
     * 654 + 43 = 697
     * 697 + 43 = 740
     * 740 - 12 = 728
     * 728 - 358 = 370
     * 370 - 12 = 358
     * 358 - 358 = 0
     * 
     */
    @Test
    public void testNumberValue_TwoInstancesOneGroup() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<Object> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).flatMap(commandGroup -> {
            System.out.println("======> Got group for command: " + commandGroup.getKey());
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                return data.get("rollingCountSuccess");
            });
        }).subscribe(ts);

        stream.onNext(getCinematchCommandInstanceStream(12345, scheduler), 0);
        stream.onNext(getCinematchCommandInstanceStream(23456, scheduler), 0);
        stream.onCompleted(100);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect 1 command
        assertEquals(1, numGroups.get());
        // the expected deltas for rollingCountSuccess (2 instances of same data grouped together)
        ts.assertReceivedOnNext(Arrays.asList(327L, 654L, 697L, 740L, 728L, 370L, 358L, 0L));
    }

    /**
     * 
     * Each instance emits =>
     * 
     * Group 1: 327, 370, 358 => deltas: 327, 43, -12, -358 (onComplete)
     * Group 2: 617, 614, 585 => deltas: 617, -3, -29, -585 (onComplete)
     * 
     * Group1 =>
     * 
     * 327, 327, 370, 370, 358, 358
     * 
     * 0 + 327 = 327
     * 327 + 327 = 654
     * 654 + 43 = 697
     * 697 + 43 = 740
     * 740 - 12 = 728
     * 728 - 358 = 370
     * 370 - 12 = 358
     * 358 - 358 = 0
     * 
     * Group 2 =>
     * 
     * 617, 617, 614, 614, 585, 585
     * 
     * 0 + 617 = 617
     * 617 + 617 = 1234
     * 1234 - 3 = 1231
     * 1231 - 3 = 1228
     * 1228 - 29 = 1199
     * 1199 - 585 = 614
     * 614 - 29 = 585
     * 585 - 585 = 0
     * 
     * Interleaved because 2 groups:
     * 
     * 327, 654, 617, 1234, 697, 740, 1231, 1228, 728, 716, 1199, 1170
     * 
     */
    @Test
    public void testNumberValue_TwoInstancesTwoGroups() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<Object> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).flatMap(commandGroup -> {
            System.out.println("======> Got group for command: " + commandGroup.getKey());
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                return data.get("rollingCountSuccess");
            });
        }).subscribe(ts);

        stream.onNext(getSubscriberAndCinematchCommandInstanceStream(12345, scheduler), 0);
        stream.onNext(getSubscriberAndCinematchCommandInstanceStream(23456, scheduler), 5);
        stream.onCompleted(100);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect 2 commands
        assertEquals(2, numGroups.get());
        // the expected deltas for rollingCountSuccess (2 instances of same data grouped together)
        ts.assertReceivedOnNext(Arrays.asList(327L, 654L, 617L, 1234L, 697L, 740L, 1231L, 1228L, 728L, 716L, 1199L, 614L, 358L, 585L, 0L, 0L));
    }

    @Test
    public void testStringValue_OneInstanceOneGroup() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<Object> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).flatMap(commandGroup -> {
            System.out.println("======> Got group for command: " + commandGroup.getKey());
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                return ((AggregateString) data.get("isCircuitBreakerOpen")).toJson();
            });
        }).subscribe(ts);

        stream.onNext(getCinematchCommandInstanceStream(12345, scheduler), 5);
        stream.onCompleted(100);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect a single instance
        assertEquals(1, numGroups.get());
        // the expected deltas for rollingCountSuccess
        ts.assertReceivedOnNext(Arrays.asList("{\"false\":1}", "{\"false\":1}", "{\"true\":1}", "{}"));
    }

    @Test
    public void testStringValue_TwoInstancesOneGroup() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<Object> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).flatMap(commandGroup -> {
            System.out.println("======> Got group for command: " + commandGroup.getKey());
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                return ((AggregateString) data.get("isCircuitBreakerOpen")).toJson();
            });
        }).subscribe(ts);

        stream.onNext(getCinematchCommandInstanceStream(12345, scheduler), 0);
        stream.onNext(getCinematchCommandInstanceStream(23456, scheduler), 0);
        stream.onCompleted(100);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect 1 command
        assertEquals(1, numGroups.get());
        // the expected deltas for rollingCountSuccess (2 instances of same data grouped together)
        ts.assertReceivedOnNext(Arrays.asList("{\"false\":1}", "{\"false\":2}", "{\"false\":2}", "{\"false\":2}", "{\"false\":1,\"true\":1}", "{\"false\":1}", "{\"true\":1}", "{}"));
    }

    /*
     * Test that an instance dropping correctly removes the data
     */
    @Test
    public void testInstanceRemovalStringValue() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<AggregateString> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).<AggregateString> flatMap(commandGroup -> {
            System.out.println("======> Got group for command: " + commandGroup.getKey());
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                return ((AggregateString) data.get("isCircuitBreakerOpen"));
            });
        }).subscribe(ts);

        stream.onNext(getCinematchCommandInstanceStream(12345, scheduler, 31), 0);
        stream.onNext(getCinematchCommandInstanceStream(23456, scheduler, 100), 0);
        stream.onCompleted(100);

        scheduler.advanceTimeTo(30, TimeUnit.MILLISECONDS);

        // assert we have two groups aggregated

        List<AggregateString> onNextAt30 = ts.getOnNextEvents();
        List<String> jsonAt30 = ts.getOnNextEvents().stream().map(as -> as.toJson()).collect(Collectors.toList());
        System.out.println("OnNext at 30ms -> " + jsonAt30);

        // we should have 2 instance now
        System.out.println("Instances at 30: " + onNextAt30.get(onNextAt30.size() - 1).instances());
        assertEquals(2, onNextAt30.get(onNextAt30.size() - 1).instances().size());
        // the expected deltas for rollingCountSuccess (2 instances of same data grouped together)
        assertEquals(jsonAt30, Arrays.asList("{\"false\":1}", "{\"false\":2}", "{\"false\":2}", "{\"false\":2}", "{\"false\":1,\"true\":1}", "{\"true\":2}"));

        // advance past the first stream so it onCompletes and removes itself
        scheduler.advanceTimeTo(31, TimeUnit.MILLISECONDS);
        // we should now see only 1 value
        List<AggregateString> onNextAt31 = ts.getOnNextEvents();
        List<String> jsonAt31 = ts.getOnNextEvents().stream().map(as -> as.toJson()).collect(Collectors.toList());
        System.out.println("OnNext at 31ms -> " + jsonAt31);

        // we should only have 1 instance now
        System.out.println("Instances at 31: " + onNextAt31.get(onNextAt31.size() - 1).instances());
        assertEquals(1, onNextAt31.get(onNextAt31.size() - 1).instances().size());

        assertEquals(jsonAt31, Arrays.asList("{\"false\":1}", "{\"false\":2}", "{\"false\":2}", "{\"false\":2}", "{\"false\":1,\"true\":1}", "{\"true\":2}", "{\"true\":1}"));

        // complete
        scheduler.advanceTimeTo(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect 1 command
        assertEquals(1, numGroups.get());
    }

    @Test
    public void testFields() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<Object> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).flatMap(commandGroup -> {
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                validateNumber(data, "reportingHosts");
                validateString(data, "type");
                validateString(data, "name");
                validateAggregateString(data, "group");
                validateNull(data, "currentTime");
                validateAggregateString(data, "isCircuitBreakerOpen");
                validateNumber(data, "errorPercentage");
                validateNumber(data, "errorCount");
                validateNumber(data, "requestCount");
                validateNumber(data, "rollingCountCollapsedRequests");
                validateNumber(data, "rollingCountExceptionsThrown");
                validateNumber(data, "rollingCountFailure");
                validateNumber(data, "rollingCountFallbackFailure");
                validateNumber(data, "rollingCountFallbackRejection");
                validateNumber(data, "rollingCountFallbackSuccess");
                validateNumber(data, "rollingCountResponsesFromCache");
                validateNumber(data, "rollingCountSemaphoreRejected");
                validateNumber(data, "rollingCountShortCircuited");
                validateNumber(data, "rollingCountSuccess");
                validateNumber(data, "rollingCountThreadPoolRejected");
                validateNumber(data, "rollingCountTimeout");
                validateNumber(data, "currentConcurrentExecutionCount");
                validateNumber(data, "latencyExecute_mean");
                validateNumberList(data, "latencyExecute");
                validateNumber(data, "latencyTotal_mean");
                validateNumberList(data, "latencyTotal");
                validateAggregateString(data, "propertyValue_circuitBreakerRequestVolumeThreshold");
                validateAggregateString(data, "propertyValue_circuitBreakerSleepWindowInMilliseconds");
                validateAggregateString(data, "propertyValue_circuitBreakerErrorThresholdPercentage");
                validateAggregateString(data, "propertyValue_circuitBreakerForceOpen");
                validateAggregateString(data, "propertyValue_executionIsolationStrategy");
                validateAggregateString(data, "propertyValue_executionIsolationThreadTimeoutInMilliseconds");
                validateAggregateString(data, "propertyValue_executionIsolationThreadInterruptOnTimeout");
                validateAggregateString(data, "propertyValue_executionIsolationSemaphoreMaxConcurrentRequests");
                validateAggregateString(data, "propertyValue_fallbackIsolationSemaphoreMaxConcurrentRequests");
                validateAggregateString(data, "propertyValue_requestCacheEnabled");
                validateAggregateString(data, "propertyValue_requestLogEnabled");
                validateAggregateString(data, "propertyValue_metricsRollingStatisticalWindowInMilliseconds");
                return data.get("name");
            });
        }).subscribe(ts);

        stream.onNext(getCinematchCommandInstanceStream(12345, scheduler), 0);
        stream.onNext(getCinematchCommandInstanceStream(23456, scheduler), 0);
        stream.onCompleted(100);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect 1 command
        assertEquals(1, numGroups.get());
        // the expected deltas for rollingCountSuccess (2 instances of same data grouped together)
        ts.assertReceivedOnNext(Arrays.asList("CinematchGetPredictions", "CinematchGetPredictions", "CinematchGetPredictions", "CinematchGetPredictions", "CinematchGetPredictions", "CinematchGetPredictions", "CinematchGetPredictions", "CinematchGetPredictions"));
    }

    @Test
    public void testFieldReportingHosts() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<Object> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).flatMap(commandGroup -> {
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                return data.get("reportingHosts");
            });
        }).subscribe(ts);

        stream.onNext(getCinematchCommandInstanceStream(12345, scheduler), 0);
        stream.onNext(getCinematchCommandInstanceStream(23456, scheduler), 0);
        stream.onCompleted(100);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect 1 command
        assertEquals(1, numGroups.get());
        ts.assertReceivedOnNext(Arrays.asList(1L, 2L, 2L, 2L, 2L, 1L, 1L, 0L));
    }

    @Test
    public void testField_propertyValue_circuitBreakerForceOpen() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<Object> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).flatMap(commandGroup -> {
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                return String.valueOf(data.get("propertyValue_circuitBreakerForceOpen"));
            });
        }).subscribe(ts);

        stream.onNext(getCinematchCommandInstanceStream(12345, scheduler), 0);
        stream.onNext(getCinematchCommandInstanceStream(23456, scheduler), 0);
        stream.onCompleted(100);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect 1 command
        assertEquals(1, numGroups.get());
        ts.assertReceivedOnNext(Arrays.asList("AggregateString => {\"false\":1}", 
                "AggregateString => {\"false\":2}", 
                "AggregateString => {\"false\":2}", 
                "AggregateString => {\"false\":2}", 
                "AggregateString => {\"false\":2}", 
                "AggregateString => {\"false\":1}",
                "AggregateString => {\"false\":1}",
                "AggregateString => {}"));
    }

    @Test
    public void testFieldOnStream() {
        TestScheduler scheduler = new TestScheduler();
        TestSubscriber<Object> ts = new TestSubscriber<>();
        // 20 events per instance, 10 per group
        // 80 events total
        GroupedObservable<InstanceKey, Map<String, Object>> hystrixStreamA = HystrixStreamSource.getHystrixStreamFromFileEachLineScheduledEvery10Milliseconds(HystrixStreamSource.STREAM_SUBSCRIBER_CINEMATCH_1, 12345, scheduler, 200);
        GroupedObservable<InstanceKey, Map<String, Object>> hystrixStreamB = HystrixStreamSource.getHystrixStreamFromFileEachLineScheduledEvery10Milliseconds(HystrixStreamSource.STREAM_SUBSCRIBER_CINEMATCH_1, 23456, scheduler, 200);
        GroupedObservable<InstanceKey, Map<String, Object>> hystrixStreamC = HystrixStreamSource.getHystrixStreamFromFileEachLineScheduledEvery10Milliseconds(HystrixStreamSource.STREAM_SUBSCRIBER_CINEMATCH_1, 67890, scheduler, 200);
        GroupedObservable<InstanceKey, Map<String, Object>> hystrixStreamD = HystrixStreamSource.getHystrixStreamFromFileEachLineScheduledEvery10Milliseconds(HystrixStreamSource.STREAM_SUBSCRIBER_CINEMATCH_1, 63543, scheduler, 200);

        Observable<GroupedObservable<InstanceKey, Map<String, Object>>> fullStream = Observable.just(hystrixStreamA, hystrixStreamB, hystrixStreamC, hystrixStreamD);
        StreamAggregator.aggregateGroupedStreams(fullStream).flatMap(commandGroup -> {
            System.out.println("======> Got group for command: " + commandGroup.getKey());
            return commandGroup;
        }).doOnNext(data -> {
            System.out.println("data => " + data.get("propertyValue_circuitBreakerForceOpen") + " " + data.get("name"));
        }).skip(8).doOnNext(v -> {
            // assert the count is always 4 (4 instances) on AggregateString values
                AggregateString as = (AggregateString) (v.get("propertyValue_circuitBreakerForceOpen"));
                if (!"AggregateString => {\"false\":4}".equals(as.toString())) {
                    // after the initial 1, 2, 3, 4 counting on each instance we should receive 4 always thereafter 
                    // and we skip the first 8 to get past those
                    throw new IllegalStateException("Expect the count to always be 4 but was " + as.toString());
                }
            }).subscribe(ts);

        // only got to 199 so we don't trigger completion
        scheduler.advanceTimeBy(199, TimeUnit.MILLISECONDS);

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
    }

    private void validateNumberList(Map<String, Object> data, String key) {
        Object o = data.get(key);
        if (o == null) {
            throw new IllegalStateException("Expected value: " + key);
        }
        if (!(o instanceof NumberList)) {
            throw new IllegalStateException("Expected value of '" + key + "' to be a NumberList but was: " + o.getClass().getSimpleName());
        }
    }

    private void validateNull(Map<String, Object> data, String key) {
        Object o = data.get(key);
        if (o != null) {
            throw new IllegalStateException("Did not expect value for key: " + key);
        }
    }

    private void validateAggregateString(Map<String, Object> data, String key) {
        Object o = data.get(key);
        if (o == null) {
            throw new IllegalStateException("Expected value: " + key);
        }
        if (!(o instanceof AggregateString)) {
            throw new IllegalStateException("Expected value of '" + key + "' to be a AggregateString but was: " + o.getClass().getSimpleName());
        }
    }

    private void validateString(Map<String, Object> data, String key) {
        Object o = data.get(key);
        if (o == null) {
            throw new IllegalStateException("Expected value: " + key);
        }
        if (!(o instanceof String)) {
            throw new IllegalStateException("Expected value of '" + key + "' to be a String but was: " + o.getClass().getSimpleName());
        }
    }

    private void validateNumber(Map<String, Object> data, String key) {
        Object o = data.get(key);
        if (o == null) {
            throw new IllegalStateException("Expected value: " + key);
        }
        if (!(o instanceof Number)) {
            throw new IllegalStateException("Expected value of '" + key + "' to be a Number but was: " + o.getClass().getSimpleName());
        }
    }

    /**
     * This looks for the latency values which look like this:
     * 
     * {"0":0,"25":0,"50":4,"75":11,"90":14,"95":17,"99":31,"99.5":43,"100":71}
     * {"0":0,"25":0,"50":3,"75":12,"90":17,"95":24,"99":48,"99.5":363,"100":390}
     * {"0":0,"25":0,"50":3,"75":12,"90":17,"95":24,"99":48,"99.5":363,"100":390}
     * 
     * The inner values need to be summed.
     */
    @Test
    public void testArrayMapValue_OneInstanceOneGroup() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<Object> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).flatMap(commandGroup -> {
            System.out.println("======> Got group for command: " + commandGroup.getKey());
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                return ((NumberList) data.get("latencyTotal")).toJson();
            });
        }).subscribe(ts);

        stream.onNext(getCinematchCommandInstanceStream(12345, scheduler), 5);
        stream.onCompleted(100);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect a single instance
        assertEquals(1, numGroups.get());
        // the expected deltas for rollingCountSuccess
        ts.assertReceivedOnNext(Arrays.asList("{\"0\":0,\"25\":0,\"50\":4,\"75\":11,\"90\":14,\"95\":17,\"99\":31,\"99.5\":43,\"100\":71}", "{\"0\":0,\"25\":0,\"50\":3,\"75\":12,\"90\":17,\"95\":24,\"99\":48,\"99.5\":363,\"100\":390}", "{\"0\":0,\"25\":0,\"50\":3,\"75\":12,\"90\":17,\"95\":24,\"99\":48,\"99.5\":363,\"100\":390}", "{\"0\":0,\"25\":0,\"50\":0,\"75\":0,\"90\":0,\"95\":0,\"99\":0,\"99.5\":0,\"100\":0}"));
    }

    /**
     * This looks for the latency values which look like this:
     * 
     * {"0":0,"25":0,"50":4,"75":11,"90":14,"95":17,"99":31,"99.5":43,"100":71}
     * {"0":0,"25":0,"50":3,"75":12,"90":17,"95":24,"99":48,"99.5":363,"100":390}
     * {"0":0,"25":0,"50":3,"75":12,"90":17,"95":24,"99":48,"99.5":363,"100":390}
     * 
     * The inner values need to be summed.
     */
    @Test
    public void testArrayMapValue_TwoInstanceOneGroup() {
        TestScheduler scheduler = new TestScheduler();
        TestSubject<GroupedObservable<InstanceKey, Map<String, Object>>> stream = TestSubject.create(scheduler);

        AtomicInteger numGroups = new AtomicInteger();
        TestSubscriber<Object> ts = new TestSubscriber<>();

        StreamAggregator.aggregateGroupedStreams(stream).flatMap(commandGroup -> {
            System.out.println("======> Got group for command: " + commandGroup.getKey());
            numGroups.incrementAndGet();
            return commandGroup.map(data -> {
                return ((NumberList) data.get("latencyTotal")).toJson();
            });
        }).subscribe(ts);

        stream.onNext(getCinematchCommandInstanceStream(12345, scheduler), 0);
        stream.onNext(getCinematchCommandInstanceStream(23456, scheduler), 5);
        stream.onCompleted(100);

        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        ts.awaitTerminalEvent();

        System.out.println("---------> OnErrorEvents: " + ts.getOnErrorEvents());
        if (ts.getOnErrorEvents().size() > 0) {
            ts.getOnErrorEvents().get(0).printStackTrace();
        }
        System.out.println("---------> OnNextEvents: " + ts.getOnNextEvents());
        assertEquals(0, ts.getOnErrorEvents().size());
        // we expect a single instance
        assertEquals(1, numGroups.get());
        // the expected deltas for rollingCountSuccess
        ts.assertReceivedOnNext(Arrays.asList(
                "{\"0\":0,\"25\":0,\"50\":4,\"75\":11,\"90\":14,\"95\":17,\"99\":31,\"99.5\":43,\"100\":71}",
                "{\"0\":0,\"25\":0,\"50\":8,\"75\":22,\"90\":28,\"95\":34,\"99\":62,\"99.5\":86,\"100\":142}", // 71 + 71 combination
                "{\"0\":0,\"25\":0,\"50\":7,\"75\":23,\"90\":31,\"95\":41,\"99\":79,\"99.5\":406,\"100\":461}", // 71 + 390 combination
                "{\"0\":0,\"25\":0,\"50\":6,\"75\":24,\"90\":34,\"95\":48,\"99\":96,\"99.5\":726,\"100\":780}", // 390 + 390 combination
                "{\"0\":0,\"25\":0,\"50\":6,\"75\":24,\"90\":34,\"95\":48,\"99\":96,\"99.5\":726,\"100\":780}", // 390 + 390 combination
                "{\"0\":0,\"25\":0,\"50\":3,\"75\":12,\"90\":17,\"95\":24,\"99\":48,\"99.5\":363,\"100\":390}", // 780 - 390
                "{\"0\":0,\"25\":0,\"50\":3,\"75\":12,\"90\":17,\"95\":24,\"99\":48,\"99.5\":363,\"100\":390}", // 780 - 390
                "{\"0\":0,\"25\":0,\"50\":0,\"75\":0,\"90\":0,\"95\":0,\"99\":0,\"99.5\":0,\"100\":0}"));
    }

    private GroupedObservable<InstanceKey, Map<String, Object>> getCinematchCommandInstanceStream(int instanceId, TestScheduler scheduler) {
        return getCinematchCommandInstanceStream(instanceId, scheduler, 30); // 30ms max time before onComplete
    }

    // `rollingCountSuccess` of => 327, 370, 358
    private GroupedObservable<InstanceKey, Map<String, Object>> getCinematchCommandInstanceStream(int instanceId, TestScheduler scheduler, int time) {
        return HystrixStreamSource.getHystrixStreamFromFileEachLineScheduledEvery10Milliseconds(HystrixStreamSource.STREAM_CINEMATCH, instanceId, scheduler, time);
    }

    // `rollingCountSuccess` of => 617, 614, 585
    private GroupedObservable<InstanceKey, Map<String, Object>> getSubscriberCommandInstanceStream(int instanceId, TestScheduler scheduler) {
        return HystrixStreamSource.getHystrixStreamFromFileEachLineScheduledEvery10Milliseconds(HystrixStreamSource.STREAM_SUBSCRIBER, instanceId, scheduler, 30);
    }

    // `rollingCountSuccess` of => 327, 617, 370, 614, 358, 585
    private GroupedObservable<InstanceKey, Map<String, Object>> getSubscriberAndCinematchCommandInstanceStream(int instanceId, TestScheduler scheduler) {
        return HystrixStreamSource.getHystrixStreamFromFileEachLineScheduledEvery10Milliseconds(HystrixStreamSource.STREAM_SUBSCRIBER_CINEMATCH_1, instanceId, scheduler, 60);
    }

    private Map<String, Object> newMapInitializedWithInstanceKey() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("InstanceKey", InstanceKey.create(98765));
        return m;
    }
    
    @Test
    public void testDeltaNumberNew() {
        Map<String, Object> mCurrent = newMapInitializedWithInstanceKey();
        mCurrent.put("a", 1);
        mCurrent.put("b", 2);
        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(Collections.emptyMap(), mCurrent);
        assertEquals(1l, d.get("a"));
        assertEquals(2l, d.get("b"));

        Map<String, Object> s = StreamAggregator.sumOfDelta(newMapInitializedWithInstanceKey(), d);
        assertEquals(1l, s.get("a"));
        assertEquals(2l, s.get("b"));
    }

    @Test
    public void testDeltaNumber1() {
        Map<String, Object> mPrevious = newMapInitializedWithInstanceKey();
        mPrevious.put("a", 1);
        mPrevious.put("b", 2);

        Map<String, Object> mCurrent = newMapInitializedWithInstanceKey();
        mCurrent.put("a", 3);
        mCurrent.put("b", 1);

        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(mPrevious, mCurrent);
        assertEquals(2l, d.get("a"));
        assertEquals(-1l, d.get("b"));

        Map<String, Object> s = StreamAggregator.sumOfDelta(mPrevious, d);
        assertEquals(3l, s.get("a"));
        assertEquals(1l, s.get("b"));
    }

    @Test
    public void testDeltaNumber2() {
        Map<String, Object> mPrevious = newMapInitializedWithInstanceKey();
        mPrevious.put("a", 4);
        mPrevious.put("b", 3);

        Map<String, Object> mCurrent = newMapInitializedWithInstanceKey();
        mCurrent.put("a", 2);
        mCurrent.put("b", 2);

        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(mPrevious, mCurrent);
        assertEquals(-2l, d.get("a"));
        assertEquals(-1l, d.get("b"));

        Map<String, Object> s = StreamAggregator.sumOfDelta(mPrevious, d);
        assertEquals(2l, s.get("a"));
        assertEquals(2l, s.get("b"));
    }

    @Test
    public void testDeltaNumberRemove() {
        Map<String, Object> mPrevious = newMapInitializedWithInstanceKey();
        mPrevious.put("a", 4);
        mPrevious.put("b", 3);

        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(mPrevious, Collections.emptyMap());
        assertEquals(-4l, d.get("a"));
        assertEquals(-3l, d.get("b"));

        Map<String, Object> s = StreamAggregator.sumOfDelta(mPrevious, d);
        assertEquals(0l, s.get("a"));
        assertEquals(0l, s.get("b"));
    }
    
    @Test
    public void testDeltaNumberRemoveWithEmptyMapHavingInstanceKey() {
        Map<String, Object> mPrevious = newMapInitializedWithInstanceKey();
        mPrevious.put("a", 4);
        mPrevious.put("b", 3);

        Map<String, Object> mCurrent = newMapInitializedWithInstanceKey();

        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(mPrevious, mCurrent);
        assertEquals(-4l, d.get("a"));
        assertEquals(-3l, d.get("b"));

        Map<String, Object> s = StreamAggregator.sumOfDelta(mPrevious, d);
        assertEquals(0l, s.get("a"));
        assertEquals(0l, s.get("b"));
    }

    @Test
    public void testDeltaBooleanNew() {
        Map<String, Object> mCurrent = newMapInitializedWithInstanceKey();
        mCurrent.put("a", Boolean.TRUE);
        mCurrent.put("b", Boolean.FALSE);
        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(Collections.emptyMap(), mCurrent);
        String[] as = (String[]) d.get("a");
        String[] bs = (String[]) d.get("b");
        assertArrayEquals(new String[] { "true" }, as);
        assertArrayEquals(new String[] { "false" }, bs);

        Map<String, Object> s = StreamAggregator.sumOfDelta(new LinkedHashMap<>(), d);
        assertEquals("AggregateString => {\"true\":1}", s.get("a").toString());
        assertEquals("AggregateString => {\"false\":1}", s.get("b").toString());
    }

    @Test
    public void testDeltaBoolean1() {
        Map<String, Object> mPrevious = newMapInitializedWithInstanceKey();
        mPrevious.put("a", Boolean.TRUE);
        mPrevious.put("b", Boolean.FALSE);

        Map<String, Object> mCurrent = newMapInitializedWithInstanceKey();
        mCurrent.put("a", Boolean.TRUE);
        mCurrent.put("b", Boolean.TRUE);

        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(mPrevious, mCurrent);
        String[] as = (String[]) d.get("a");
        String[] bs = (String[]) d.get("b");
        assertArrayEquals(new String[] { "true", "true" }, as);
        assertArrayEquals(new String[] { "false", "true" }, bs);

        Map<String, Object> state = newMapInitializedWithInstanceKey();
        state.put("a", AggregateString.create("true", InstanceKey.create(98765)));
        state.put("b", AggregateString.create("false", InstanceKey.create(98765)));

        Map<String, Object> s = StreamAggregator.sumOfDelta(state, d);
        assertEquals("AggregateString => {\"true\":1}", s.get("a").toString()); // same instanceId so count == 1
        assertEquals("AggregateString => {\"true\":1}", s.get("b").toString());
    }

    @Test
    public void testDeltaBooleanRemove() {
        Map<String, Object> mPrevious = newMapInitializedWithInstanceKey();
        mPrevious.put("a", Boolean.TRUE);
        mPrevious.put("b", Boolean.FALSE);

        Map<String, Object> mCurrent = newMapInitializedWithInstanceKey();

        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(mPrevious, mCurrent);
        String[] as = (String[]) d.get("a");
        String[] bs = (String[]) d.get("b");
        assertArrayEquals(new String[] { "true", null }, as);
        assertArrayEquals(new String[] { "false", null }, bs);

        Map<String, Object> state = newMapInitializedWithInstanceKey();
        state.put("a", AggregateString.create("true", InstanceKey.create(98765)));
        state.put("b", AggregateString.create("false", InstanceKey.create(98765)));

        Map<String, Object> s = StreamAggregator.sumOfDelta(state, d);
        assertEquals("AggregateString => {}", s.get("a").toString()); // same instanceId so count == 1
        assertEquals("AggregateString => {}", s.get("b").toString());
    }
    
    @Test
    public void testDeltaNumberListNew() {
        Map<String, Object> mCurrent = newMapInitializedWithInstanceKey();
        Map<String, Object> v = new HashMap<>();
        v.put("100", 99);
        mCurrent.put("a", v);
        
        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(Collections.emptyMap(), mCurrent);
        assertEquals(99l, ((NumberList)d.get("a")).get("100").longValue());

        Map<String, Object> s = StreamAggregator.sumOfDelta(newMapInitializedWithInstanceKey(), d);
        assertEquals(99l, ((NumberList)s.get("a")).get("100").longValue());
    }

    @Test
    public void testDeltaNumberList1() {
        Map<String, Object> mPrevious = newMapInitializedWithInstanceKey();
        Map<String, Object> v = new HashMap<>();
        v.put("100", 99);
        mPrevious.put("a", v);

        Map<String, Object> mCurrent = newMapInitializedWithInstanceKey();
        Map<String, Object> v2 = new HashMap<>();
        v2.put("100", 97);
        mCurrent.put("a", v2);

        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(mPrevious, mCurrent);
        assertEquals(-2l, ((NumberList)d.get("a")).get("100").longValue());

        Map<String, Object> initial = StreamAggregator.previousAndCurrentToDelta(Collections.emptyMap(), mPrevious);
        Map<String, Object> s = StreamAggregator.sumOfDelta(initial, d);
        assertEquals(97l, ((NumberList)s.get("a")).get("100").longValue());
    }

    @Test
    public void testDeltaNumberList2() {
        Map<String, Object> mPrevious = newMapInitializedWithInstanceKey();
        Map<String, Number> v = new HashMap<>();
        v.put("100", 90);
        mPrevious.put("a", v);

        Map<String, Object> mCurrent = newMapInitializedWithInstanceKey();
        Map<String, Number> v2 = new HashMap<>();
        v2.put("100", 99);
        mCurrent.put("a", v2);

        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(mPrevious, mCurrent);
        assertEquals(9l, ((NumberList)d.get("a")).get("100").longValue());

        Map<String, Object> initial = StreamAggregator.previousAndCurrentToDelta(Collections.emptyMap(), mPrevious);
        Map<String, Object> s = StreamAggregator.sumOfDelta(initial, d);
        assertEquals(99l, ((NumberList)s.get("a")).get("100").longValue());
    }

    @Test
    public void testDeltaNumberListRemove() {
        Map<String, Object> mPrevious = newMapInitializedWithInstanceKey();
        Map<String, Number> v = new HashMap<>();
        v.put("100", 99);
        mPrevious.put("a", v);

        Map<String, Object> d = StreamAggregator.previousAndCurrentToDelta(mPrevious, Collections.emptyMap());
        System.out.println("d: " + d);
        assertEquals(-99l, ((NumberList)d.get("a")).get("100").longValue());

        Map<String, Object> initial = StreamAggregator.previousAndCurrentToDelta(Collections.emptyMap(), mPrevious);
        Map<String, Object> s = StreamAggregator.sumOfDelta(initial, d);
        assertEquals(0l, ((NumberList)s.get("a")).get("100").longValue());
    }

}