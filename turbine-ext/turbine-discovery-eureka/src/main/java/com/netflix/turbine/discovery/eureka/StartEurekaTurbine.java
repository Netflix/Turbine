package com.netflix.turbine.discovery.eureka;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.text.sse.ServerSentEvent;

import com.netflix.turbine.Turbine;
import com.netflix.turbine.internal.JsonUtility;

public class StartEurekaTurbine {

    public static void main(String[] args) {
        //        Turbine.aggregateHttpSSE(
        //                EurekaStreamDiscovery.create("api", "http://"
        //                        + EurekaStreamDiscovery.HOSTNAME
        //                        + ":8077/eventbus.stream?topic=hystrix-metrics&delay=1000"))
        //                .flatMap(o -> o)
        //                .map(data -> JsonUtility.mapToJson(data))
        //                .toBlocking()
        //                .forEach(System.out::println);
        //        
        //        
        System.out.println("Starting server on 8888");
        RxNetty.createHttpServer(8888, (request, response) -> {
            System.out.println("Starting aggregation => " + request.getUri());
            System.out.println("headers: " + response.getHeaders());
            response.getHeaders().setHeader("Content-Type", "text/event-stream");
            return Turbine.aggregateHttpSSE(
                    EurekaStreamDiscovery.create("api", "http://"
                            + EurekaStreamDiscovery.HOSTNAME
                            + ":8077/eventbus.stream?topic=hystrix-metrics&delay=1000"))
                    .doOnUnsubscribe(() -> System.out.println("Unsubscribing RxNetty server connection"))
                    .filter(o -> o.getKey().getKey().contains("Geo"))
                    .flatMap(o -> o)
                    .flatMap(data -> {
                        return response.writeAndFlush(new ServerSentEvent(null, null, JsonUtility.mapToJson(data)));
                    }).doOnError(t -> {
                        t.printStackTrace();
                        response.close(); // this shouldn't be needed
                        });
        }, PipelineConfigurators.<ByteBuf> sseServerConfigurator()).startAndWait();

    }

}
