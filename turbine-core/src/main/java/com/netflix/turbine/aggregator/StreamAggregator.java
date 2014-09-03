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

import static com.netflix.turbine.internal.GroupedObservableUtils.createGroupedObservable;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import rx.Observable;
import rx.observables.GroupedObservable;

import com.netflix.turbine.internal.OperatorPivot;

public class StreamAggregator {

    public static Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> aggregateGroupedStreams(Observable<GroupedObservable<InstanceKey, Map<String, Object>>> stream) {
        //                return aggregateUsingFlattenedGroupBy(stream);
        return aggregateUsingPivot(stream);
    }

    private StreamAggregator() {

    }

    /**
     * Uses pivot to retain concurrency instead of flattening and then grouping.
     * 
     * benjchristensen => as of September 2014 the pivot implementation is complicated and likely has a concurrency bug based on non-deterministic test failures.
     * 
     * @param stream
     * @return
     */
    private static Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> aggregateUsingPivot(Observable<GroupedObservable<InstanceKey, Map<String, Object>>> stream) {
        Observable<GroupedObservable<InstanceKey, GroupedObservable<TypeAndNameKey, Map<String, Object>>>> instanceThemCommand =
                stream.map((GroupedObservable<InstanceKey, Map<String, Object>> instanceStream) -> {
                    Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> instanceGroupedByCommand = instanceStream
                            .groupBy((Map<String, Object> json) -> {
                                return TypeAndNameKey.from(String.valueOf(json.get("type")), String.valueOf(json.get("name")));
                            });
                    return createGroupedObservable(instanceStream.getKey(), instanceGroupedByCommand);
                });

        return instanceThemCommand.lift(OperatorPivot.create()).map(commandGroup -> {
            // merge all instances per group into a single stream of deltas and sum them 
                return createGroupedObservable(commandGroup.getKey(), commandGroup.flatMap(instanceGroup -> {
                    return instanceGroup.startWith(Collections.<String, Object> emptyMap())
                            .buffer(2, 1)
                            .map(StreamAggregator::previousAndCurrentToDelta)
                            .filter(data -> data != null && !data.isEmpty());
                }).scan(new HashMap<String, Object>(), StreamAggregator::sumOfDelta)
                        .filter(data -> data.size() > 0));
            });
    }

    /**
     * Flatten the stream and then do nested groupBy. This matches the mental model and is simple but
     * serializes the stream to a single thread which is bad for performance.
     * 
     * TODO: Add a version like this but with a ParallelObservable
     * 
     * @param stream
     * @return
     */
    @SuppressWarnings("unused")
    private static Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> aggregateUsingFlattenedGroupBy(Observable<GroupedObservable<InstanceKey, Map<String, Object>>> stream) {
        Observable<Map<String, Object>> allData = stream.flatMap(instanceStream -> {
            return instanceStream.map((Map<String, Object> dictionary) -> {
                // instead of using a Tuple (because of object allocation) we'll just inject the key
                    dictionary.put("InstanceKey", instanceStream.getKey());
                    return dictionary;
                });
        });

        Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> byCommand = allData
                .groupBy((Map<String, Object> json) -> {
                    return TypeAndNameKey.from(String.valueOf(json.get("type")), String.valueOf(json.get("name")));
                });

        return byCommand
                .map(commandGroup -> {
                    Observable<Map<String, Object>> sumOfDeltasForAllInstancesForCommand = commandGroup
                            .groupBy((Map<String, Object> json) -> {
                                return InstanceKey.create(String.valueOf(json.get("InstanceKey")));
                            }).flatMap(instanceGroup -> {
                                // calculate and output deltas for each instance stream per command
                                    return instanceGroup
                                            .startWith(Collections.<String, Object> emptyMap())
                                            .buffer(2, 1)
                                            .map(StreamAggregator::previousAndCurrentToDelta)
                                            .filter(data -> data != null && !data.isEmpty());
                                })
                            // we now have all instance deltas merged into a single stream per command
                            // and sum them into a single stream of aggregate values
                            .scan(new HashMap<String, Object>(), StreamAggregator::sumOfDelta)
                            .filter(data -> data.size() > 0);

                    // we artificially wrap in a GroupedObservable that communicates the CommandKey this stream represents
                return createGroupedObservable(commandGroup.getKey(), sumOfDeltasForAllInstancesForCommand);
            });
    }

    @SuppressWarnings("unchecked")
    private static final Map<String, Object> previousAndCurrentToDelta(List<Map<String, Object>> data) {
        if (data.size() < 2) {
            // the last time through, we'll emit empty map so no change
            return Collections.emptyMap();
        }
        Map<String, Object> previous = data.get(0);
        Map<String, Object> current = data.get(1);

        if (previous.isEmpty()) {
            // the first time through it is empty so we'll emit the current to start
            Map<String, Object> seed = new LinkedHashMap<String, Object>();
            seed.put("name", current.get("name")); // we don't aggregate this, just pass it through
            seed.put("instanceId", current.get("instanceId")); // we don't aggregate this, just pass it through

            for (String key : current.keySet()) {
                if (key.equals("instanceId") || key.equals("currentTime") || key.equals("name")) {
                    continue;
                }
                Object currentValue = current.get(key);
                if (currentValue instanceof Number && !key.startsWith("propertyValue_")) {
                    // convert all numbers to Long so they are consistent
                    seed.put(key, ((Number) currentValue).longValue());
                } else if (currentValue instanceof Map) {
                    // NOTE: we are expecting maps to only contain key/value pairs with values as numbers
                    seed.put(key, NumberList.create((Map<String, Object>) currentValue));
                } else {
                    seed.put(key, new String[] { String.valueOf(currentValue) });
                }
            }
            return seed;
        } else {
            // we have previous and current so calculate delta
            Map<String, Object> delta = new LinkedHashMap<String, Object>();
            delta.put("name", current.get("name")); // we don't aggregate this, just pass it through
            delta.put("instanceId", current.get("instanceId")); // we don't aggregate this, just pass it through

            for (String key : current.keySet()) {
                if (key.equals("instanceId") || key.equals("currentTime") || key.equals("name")) {
                    continue;
                }
                Object previousValue = previous.get(key);
                Object currentValue = current.get(key);
                if (currentValue instanceof Number && !key.startsWith("propertyValue_")) {
                    if (previousValue == null) {
                        previousValue = 0;
                    }
                    Number previousValueAsNumber = (Number) previousValue;
                    if (currentValue != null) {
                        Number currentValueAsNumber = (Number) currentValue;
                        long d = (currentValueAsNumber.longValue() - previousValueAsNumber.longValue());
                        delta.put(key, d);
                    }
                } else if (currentValue instanceof Map) {
                    if (previousValue == null) {
                        delta.put(key, NumberList.create((Map<String, Object>) currentValue));
                    } else {
                        delta.put(key, NumberList.delta((Map<String, Object>) currentValue, (Map<String, Object>) previousValue));
                    }
                } else {
                    delta.put(key, new String[] { String.valueOf(previousValue), String.valueOf(currentValue) });

                }
            }
            return delta;
        }
    }

    private static Map<String, Object> sumOfDelta(Map<String, Object> state, Map<String, Object> delta) {
        InstanceKey instanceId = InstanceKey.create(String.valueOf(delta.get("instanceId")));
        for (String key : delta.keySet()) {
            Object existing = state.get(key);
            Object current = delta.get(key);
            if (current instanceof Number) {
                if (existing == null) {
                    existing = 0;
                }
                Number v = (Number) existing;
                Number d = (Number) delta.get(key);
                state.put(key, v.longValue() + d.longValue());
            } else if (current instanceof NumberList) {
                if (existing == null) {
                    state.put(key, current);
                } else {
                    state.put(key, ((NumberList) existing).sum((NumberList) current));
                }
            } else {
                Object o = delta.get(key);
                if (o instanceof String[]) {
                    String[] vs = (String[]) o;
                    if (vs.length == 1) {
                        Object previousAggregateString = state.get(key);
                        if (previousAggregateString instanceof AggregateString) {
                            state.put(key, ((AggregateString) previousAggregateString).update(null, vs[0], instanceId));
                        } else {
                            state.put(key, AggregateString.create(vs[0], instanceId));
                        }
                    } else {
                        // it should always be AggregateString here since that's all we add above
                        AggregateString pas = (AggregateString) state.get(key);
                        state.put(key, pas.update(vs[0], vs[1], instanceId));
                    }
                } else {
                    state.put(key, String.valueOf(o));
                }

            }
        }
        return state;
    }

}
