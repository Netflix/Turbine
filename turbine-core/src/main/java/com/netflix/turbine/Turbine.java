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

import static com.netflix.turbine.internal.GroupedObservableUtils.createGroupedObservable;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.text.sse.ServerSentEvent;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;
import rx.observables.GroupedObservable;

import com.netflix.turbine.aggregator.InstanceKey;
import com.netflix.turbine.aggregator.StreamAggregator;
import com.netflix.turbine.aggregator.TypeAndNameKey;
import com.netflix.turbine.discovery.StreamAction;
import com.netflix.turbine.discovery.StreamAction.ActionType;
import com.netflix.turbine.discovery.StreamDiscovery;
import com.netflix.turbine.internal.JsonUtility;

public class Turbine {

    public static void main(String[] args) {
        try {

            URI turbine = new URI("http://ec2-54-87-56-18.compute-1.amazonaws.com:7101/turbine.stream?cluster=api-prod-c0us.ca");
            Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> aggregateHttp = aggregateHttpSSE(turbine);

            //            Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> aggregateHttp = aggregateHttpSSE(
            //                    new URI("http://ec2-54-90-87-244.compute-1.amazonaws.com:8077/eventbus.stream?topic=hystrix-metrics&delay=1000"),
            //                    new URI("http://ec2-54-80-107-168.compute-1.amazonaws.com:8077/eventbus.stream?topic=hystrix-metrics&delay=1000"));

            //            AtomicInteger count = new AtomicInteger();
            //            aggregateHttp.flatMap(o -> o)
            //                    .toBlocking().forEach(map -> {
            //                        if (count.incrementAndGet() % 1000 == 0) {
            //                            System.out.println("Received: " + count.get());
            //                        }
            //                        //                        System.out.println("data => " + JsonUtility.mapToJson(map));
            //                        });

            System.out.println("Starting server on 8888");
            RxNetty.createHttpServer(8888, (request, response) -> {
                System.out.println("Starting aggregation => " + request.getUri());
                System.out.println("headers: " + response.getHeaders());
                response.getHeaders().setHeader("Content-Type", "text/event-stream");
                return aggregateHttp
                        .doOnUnsubscribe(() -> System.out.println("Unsubscribing RxNetty server connection"))
                        .flatMap(o -> o)
                        .flatMap(data -> {
                            return response.writeAndFlush(new ServerSentEvent(null, null, JsonUtility.mapToJson(data)));
                        }).doOnError(t -> {
                            t.printStackTrace();
                            response.close(); // this shouldn't be needed
                            });
            }, PipelineConfigurators.<ByteBuf> sseServerConfigurator()).startAndWait();

        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> aggregateHttpSSE(URI... uris) {
        return aggregateHttpSSE(() -> {
            return Observable.from(uris).map(uri -> StreamAction.create(ActionType.ADD, uri)).concatWith(Observable.never()); // never() as we don't want to end
        });
    }

    /**
     * Aggregate multiple HTTP Server-Sent Event streams into one stream with the values summed.
     * <p>
     * The returned data must be JSON data that contains the following keys:
     * <p>
     * instanceId => Unique instance representing each stream to be merged, such as the instanceId of the server the stream is from.
     * type => The type of data such as HystrixCommand or HystrixThreadPool if aggregating Hystrix metrics.
     * name => Name of a group of metrics to be aggregated, such as a HystrixCommand name if aggregating Hystrix metrics.
     * 
     * 
     * @param uri
     * @return
     */
    public static Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> aggregateHttpSSE(StreamDiscovery discovery) {
        Observable<StreamAction> streamActions = discovery.getInstanceList().publish().refCount();
        Observable<StreamAction> streamAdds = streamActions.filter(a -> a.getType() == ActionType.ADD);
        Observable<StreamAction> streamRemoves = streamActions.filter(a -> a.getType() == ActionType.REMOVE);

        Observable<GroupedObservable<InstanceKey, Map<String, Object>>> streamPerInstance =
                streamAdds.map(streamAction -> {
                    URI uri = streamAction.getUri();
                    Observable<Map<String, Object>> io = Observable.defer(() -> {
                        System.out.println("Aggregate Stream from URI: " + uri.toASCIIString());
                        return RxNetty.createHttpClient(uri.getHost(), uri.getPort(), PipelineConfigurators.<ByteBuf> sseClientConfigurator())
                                .submit(HttpClientRequest.createGet(uri.toASCIIString()))
                                .doOnUnsubscribe(() -> System.out.println("unsubscribing RxNetty client: " + uri))
                                .takeUntil(streamRemoves.filter(a -> a.getUri().equals(streamAction.getUri()))) // unsubscribe when we receive a remove event
                                .flatMap(response -> {
                                    System.out.println("Response => " + response.getStatus().code());
                                    if (response.getStatus().code() != 200) {
                                        return Observable.error(new RuntimeException("Failed to connect: " + response.getStatus()));
                                    }
                                    AtomicInteger count = new AtomicInteger();
                                    return response.getContent()
                                            .map(sse -> JsonUtility.jsonToMap(sse.getEventData()))
                                            .doOnNext(n -> {
                                                if (count.incrementAndGet() % 1000 == 0) {
                                                    System.out.println("    source: " + count.get());
                                                }
                                            });
                                });
                    });

                    return createGroupedObservable(InstanceKey.create(uri.toASCIIString()), io);
                });

        return StreamAggregator.aggregateGroupedStreams(streamPerInstance);
    }
    /**
     * Aggregate multiple HTTP URIs
     * 
     * @param uri
     * @return
     */
    //    public static Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> aggregateHttp(java.net.URI... uri) {
    //
    //    }
}
