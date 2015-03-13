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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import rx.Observable;
import rx.observables.GroupedObservable;

public class StreamAggregator {

    public static Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> aggregateGroupedStreams(Observable<GroupedObservable<InstanceKey, Map<String, Object>>> stream) {
        return aggregateUsingFlattenedGroupBy(stream);
    }

    private StreamAggregator() {

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
            return instanceStream
                    .map((Map<String, Object> event) -> {
                        event.put("InstanceKey", instanceStream.getKey());
                        event.put("TypeAndName", TypeAndNameKey.from(String.valueOf(event.get("type")), String.valueOf(event.get("name"))));
                        return event;
                    })
                    .compose(is -> {
                        return tombstone(is, instanceStream.getKey());
                    });
        });

        Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> byCommand = allData
                .groupBy((Map<String, Object> event) -> {
                    return (TypeAndNameKey) event.get("TypeAndName");
                });

        return byCommand
                .map(commandGroup -> {
                    Observable<Map<String, Object>> sumOfDeltasForAllInstancesForCommand = commandGroup
                            .groupBy((Map<String, Object> json) -> {
                                return json.get("InstanceKey");
                            }).flatMap(instanceGroup -> {
                                // calculate and output deltas for each instance stream per command
                                    return instanceGroup
                                            .takeUntil(d -> d.containsKey("tombstone"))
                                            .startWith(Collections.<String, Object> emptyMap())
                                            .map(data -> {
                                                if (data.containsKey("tombstone")) {
                                                    return Collections.<String, Object> emptyMap();
                                                } else {
                                                    return data;
                                                }
                                            })
                                            .buffer(2, 1)
                                            .filter(list -> list.size() == 2)
                                            .map(StreamAggregator::previousAndCurrentToDelta)
                                            .filter(data -> data != null && !data.isEmpty());
                                })
                            // we now have all instance deltas merged into a single stream per command
                            // and sum them into a single stream of aggregate values
                            .scan(new HashMap<String, Object>(), StreamAggregator::sumOfDelta)
                            .skip(1);

                    // we artificially wrap in a GroupedObservable that communicates the CommandKey this stream represents
                return GroupedObservable.from(commandGroup.getKey(), sumOfDeltasForAllInstancesForCommand);
            });
    }

    /**
     * Append tombstone events to each instanceStream when they terminate so that the last values from the stream
     * will be removed from all aggregates down stream.
     * 
     * @param instanceStream
     * @param instanceKey
     */
    private static Observable<Map<String, Object>> tombstone(Observable<Map<String, Object>> instanceStream, InstanceKey instanceKey) {
        return instanceStream.publish(is -> {
            Observable<Map<String, Object>> tombstone = is
                    // collect all unique "TypeAndName" keys
                    .collect(() -> new HashSet<TypeAndNameKey>(), (listOfTypeAndName, event) -> {
                        listOfTypeAndName.add((TypeAndNameKey) event.get("TypeAndName"));
                    })
                    // when instanceStream completes, emit a "tombstone" for each "TypeAndName" in the HashSet collected above
                    .flatMap(listOfTypeAndName -> {
                        return Observable.from(listOfTypeAndName)
                                .map(typeAndName -> {
                                    Map<String, Object> tombstoneForTypeAndName = new LinkedHashMap<>();
                                    tombstoneForTypeAndName.put("tombstone", "true");
                                    tombstoneForTypeAndName.put("InstanceKey", instanceKey);
                                    tombstoneForTypeAndName.put("TypeAndName", typeAndName);
                                    tombstoneForTypeAndName.put("name", typeAndName.getName());
                                    tombstoneForTypeAndName.put("type", typeAndName.getType());
                                    return tombstoneForTypeAndName;
                                });
                    });
            
            // concat the tombstone events to the end of the original stream
            return is.mergeWith(tombstone);
        });
    }

    /**
     * This expects to receive a list of 2 items. If it is a different size it will throw an exception.
     * 
     * @param data
     * @return
     */
    /* package for unit tests */static final Map<String, Object> previousAndCurrentToDelta(List<Map<String, Object>> data) {
        if (data.size() == 2) {
            Map<String, Object> previous = data.get(0);
            Map<String, Object> current = data.get(1);
            return previousAndCurrentToDelta(previous, current);
        } else {
            throw new IllegalArgumentException("Must be list of 2 items");
        }

    }

    @SuppressWarnings("unchecked")
    /* package for unit tests */static final Map<String, Object> previousAndCurrentToDelta(Map<String, Object> previous, Map<String, Object> current) {
        if (previous.isEmpty()) {
            // the first time through it is empty so we'll emit the current to start
            Map<String, Object> seed = new LinkedHashMap<String, Object>();
            initMapWithIdentifiers(current, seed);

            for (String key : current.keySet()) {
                if (isIdentifierKey(key)) {
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
        } else if (current.isEmpty() || containsOnlyIdentifiers(current)) {
            // we are terminating so the delta we emit needs to remove everything
            Map<String, Object> delta = new LinkedHashMap<String, Object>();
            initMapWithIdentifiers(previous, delta);

            for (String key : previous.keySet()) {
                if (isIdentifierKey(key)) {
                    continue;
                }
                Object previousValue = previous.get(key);
                if (previousValue instanceof Number && !key.startsWith("propertyValue_")) {
                    Number previousValueAsNumber = (Number) previousValue;
                    long d = -previousValueAsNumber.longValue();
                    delta.put(key, d);
                } else if (previousValue instanceof Map) {
                    delta.put(key, NumberList.deltaInverse((Map<String, Object>) previousValue));
                } else {
                    delta.put(key, new String[] { String.valueOf(previousValue), null });
                }
            }
            return delta;
        } else {
            // we have previous and current so calculate delta
            Map<String, Object> delta = new LinkedHashMap<String, Object>();
            initMapWithIdentifiers(current, delta);

            for (String key : current.keySet()) {
                if (isIdentifierKey(key)) {
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

    private static boolean isIdentifierKey(String key) {
        return key.equals("InstanceKey") || key.equals("TypeAndName") || key.equals("instanceId") || key.equals("currentTime") || key.equals("name") || key.equals("type");
    }

    private static boolean containsOnlyIdentifiers(Map<String, Object> m) {
        for (String k : m.keySet()) {
            if (!isIdentifierKey(k)) {
                return false;
            }
        }
        return true;
    }

    private static void initMapWithIdentifiers(Map<String, Object> source, Map<String, Object> toInit) {
        toInit.put("InstanceKey", source.get("InstanceKey")); // we don't aggregate this, just pass it through
        toInit.put("TypeAndName", source.get("TypeAndName")); // we don't aggregate this, just pass it through
        toInit.put("instanceId", source.get("instanceId")); // we don't aggregate this, just pass it through
        toInit.put("name", source.get("name")); // we don't aggregate this, just pass it through
        toInit.put("type", source.get("type")); // we don't aggregate this, just pass it through
    }

    /* package for unit tests */static Map<String, Object> sumOfDelta(Map<String, Object> state, Map<String, Object> delta) {
        InstanceKey instanceId = (InstanceKey) delta.get("InstanceKey");
        if (instanceId == null) {
            throw new RuntimeException("InstanceKey can not be null");
        }
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
