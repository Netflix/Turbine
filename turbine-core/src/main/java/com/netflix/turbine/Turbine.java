/**
 * Copyright 2014 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.turbine;

import com.netflix.turbine.aggregator.InstanceKey;
import com.netflix.turbine.aggregator.StreamAggregator;
import com.netflix.turbine.aggregator.TypeAndNameKey;
import com.netflix.turbine.discovery.StreamAction;
import com.netflix.turbine.discovery.StreamAction.ActionType;
import com.netflix.turbine.discovery.StreamDiscovery;
import com.netflix.turbine.internal.JsonUtility;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.text.sse.ServerSentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.observables.GroupedObservable;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.netflix.turbine.internal.RequestCreator.createRequest;

public class Turbine {

    private static final Logger logger = LoggerFactory.getLogger(Turbine.class);

    public static void startServerSentEventServer(int port, Observable<GroupedObservable<TypeAndNameKey, Map<String, Object>>> streams) {
        logger.info("Turbine => Starting server on " + port);

        // multicast so multiple concurrent subscribers get the same stream
        Observable<Map<String, Object>> publishedStreams = streams
                .doOnUnsubscribe(() -> logger.info("Turbine => Unsubscribing aggregation."))
                .doOnSubscribe(() -> logger.info("Turbine => Starting aggregation"))
                .flatMap(o -> o).publish().refCount();

        RxNetty.createHttpServer(port, (request, response) -> {
            logger.info("Turbine => SSE Request Received");
            response.getHeaders().setHeader("Content-Type", "text/event-stream");
            return publishedStreams
                    .doOnUnsubscribe(() -> logger.info("Turbine => Unsubscribing RxNetty server connection"))
                    .flatMap(data -> {
                        return response.writeAndFlush(new ServerSentEvent(null, null, JsonUtility.mapToJson(data)));
                    });
        }, PipelineConfigurators.<ByteBuf>sseServerConfigurator()).startAndWait();
    }

    public static void startServerSentEventServer(int port, StreamDiscovery discovery) {
        startServerSentEventServer(port, aggregateHttpSSE(discovery));
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
                        Observable<Map<String, Object>> flatMap = RxNetty.createHttpClient(uri.getHost(), uri.getPort(), PipelineConfigurators.<ByteBuf>sseClientConfigurator())
                                .submit(createRequest(uri))
                                .flatMap(response -> {
                                    if (response.getStatus().code() != 200) {
                                        return Observable.error(new RuntimeException("Failed to connect: " + response.getStatus()));
                                    }
                                    return response.getContent()
                                            .doOnSubscribe(() -> logger.info("Turbine => Aggregate Stream from URI: " + uri.toASCIIString()))
                                            .doOnUnsubscribe(() -> logger.info("Turbine => Unsubscribing Stream: " + uri))
                                            .takeUntil(streamRemoves.filter(a -> a.getUri().equals(streamAction.getUri()))) // unsubscribe when we receive a remove event
                                            .map(sse -> JsonUtility.jsonToMap(sse.getEventData()));
                                });
                        // eclipse is having issues with type inference so breaking up 
                        return flatMap.retryWhen(attempts -> {
                            return attempts.flatMap(e -> {
                                return Observable.timer(1, TimeUnit.SECONDS)
                                        .doOnEach(n -> logger.info("Turbine => Retrying connection to: " + uri));
                            });
                        });

                    });

                    return GroupedObservable.from(InstanceKey.create(uri.toASCIIString()), io);
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
